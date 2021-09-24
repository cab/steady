use std::num::NonZeroUsize;

use redis::{AsyncCommands, FromRedisValue, RedisWrite, ToRedisArgs};
use tracing::warn;

use crate::{jobs::JobDefinition, scheduler::QueueName, Result};

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

    async fn schedule(&self, job_def: &JobDefinition) -> Result<()> {
        let mut connection = self.redis_client.get_async_connection().await?;
        let () = connection
            .lpush(&job_def.queue, job_definition_to_redis_args(job_def)?)
            .await?;
        Ok(())
    }
}

impl ToRedisArgs for QueueName {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let key = format!("queue:{}", self.as_str());
        out.write_arg(key.as_bytes());
    }
}
