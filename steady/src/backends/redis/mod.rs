mod lock;

use chrono::{DateTime, Utc};
use redis::{aio::ConnectionLike, AsyncCommands, FromRedisValue, RedisWrite, ToRedisArgs};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use tracing::{debug, error, trace, warn};

use crate::{jobs::JobDefinition, QueueName, Result};

#[derive(Debug, Clone)]
pub struct Backend {
    redis_client: redis::Client,
}

impl FromRedisValue for JobDefinition {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let bytes = <Vec<u8> as FromRedisValue>::from_redis_value(v)?;
        let def = bincode::deserialize::<Self>(&bytes)
            // todo better error message
            .map_err(|_e| (redis::ErrorKind::TypeError, "bincode failed"))?;
        Ok(def)
    }
}

impl Backend {
    pub fn new(redis_url: &str) -> Result<Self> {
        let redis_client = redis::Client::open(redis_url)?;
        Ok(Self { redis_client })
    }
}

fn job_definition_to_redis_args(def: &JobDefinition) -> Result<impl ToRedisArgs> {
    let bytes = bincode::serialize(def)?;
    Ok(bytes)
}

#[async_trait::async_trait]
impl super::Backend for Backend {
    async fn pull(&self, queue: &QueueName, count: NonZeroUsize) -> Result<Vec<JobDefinition>> {
        let mut connection = self.redis_client.get_async_connection().await?;
        // let job_defs = connection
        //     .rpop::<_, Vec<JobDefinition>>(queue, Some(count))
        //     .await?;
        let mut job_defs = Vec::new();
        for _ in 0..count.get() {
            match connection
                .rpop::<_, Option<JobDefinition>>(queue, None)
                .await
            {
                Ok(Some(job_def)) => job_defs.push(job_def),
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    warn!("failed to rpop: {}", e);
                    break;
                }
            }
        }
        Ok(job_defs)
    }

    async fn pull_scheduled(&self, count: NonZeroUsize) -> Result<Vec<JobDefinition>> {
        let mut connection = self.redis_client.get_async_connection().await?;
        let all_jobs = connection
            // TODO remove "scheduled" as magic name
            .zrangebyscore_limit::<_, _, _, Vec<JobDefinition>>(
                "scheduled",
                -1,
                Utc::now().timestamp_nanos(),
                0,
                count.get() as isize,
            )
            .await?;
        trace!("found {} jobs in zrange", all_jobs.len());
        let mut jobs = Vec::new();
        for job in all_jobs {
            match connection
                .zrem::<_, _, i32>("scheduled", job_definition_to_redis_args(&job)?)
                .await
            {
                Ok(1) => {
                    // successful remove
                    jobs.push(job);
                }
                Ok(_) => {
                    // did not remove. another instance probably took it
                    // TODO better message
                    warn!("could not remove")
                }
                Err(e) => {
                    error!("could not remove: {}", e);
                }
            }
        }

        Ok(jobs)
    }

    async fn enqueue(&self, job_def: &JobDefinition, perform_at: DateTime<Utc>) -> Result<()> {
        let mut connection = self.redis_client.get_async_connection().await?;

        if perform_at <= Utc::now() {
            // perform_at is now / in the past, so we can push this straight to the relevant queue
            let () = connection
                .lpush(&job_def.queue, job_definition_to_redis_args(job_def)?)
                .await?;
        } else {
            // put it in the `scheduled` queue
            debug!("job executes in the future, placing in scheduled queue");
            let () = connection
                .zadd(
                    "scheduled",
                    job_definition_to_redis_args(job_def)?,
                    perform_at.timestamp_nanos(),
                )
                .await?;
        }
        Ok(())
    }
}

impl ToRedisArgs for QueueName {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let key = format!("steady_queue:{}", self.as_str());
        out.write_arg(key.as_bytes());
    }
}
