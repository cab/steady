use anyhow::Result;
use rjobs::{Consumer, JobHandler, MemoryBackend, QueueName, RedisBackend};
use serde::{Deserialize, Serialize};
use test_env_log::test as logtest;

const REDIS_URL: &'static str = "redis://127.0.0.1";

#[derive(Debug, Serialize, Deserialize, Default)]
struct Log {}

#[rjobs::async_trait]
impl JobHandler for Log {
    const NAME: &'static str = "log";
    type Arg = String;
    type Error = anyhow::Error;

    async fn perform(&mut self, arg: Self::Arg) -> Result<(), Self::Error> {
        tracing::info!("log! {}", arg);
        Ok(())
    }
}

#[logtest(tokio::test)]
async fn test_redis() -> Result<()> {
    let mut consumer = Consumer::new(RedisBackend::new(REDIS_URL)?)?;
    consumer.start();
    let job_id = consumer
        .schedule::<Log>(&"test, redis".to_string(), QueueName::from("default"))
        .await?;
    consumer.drain(true).await?;
    Ok(())
}

#[logtest(tokio::test)]
async fn test_memory() -> Result<()> {
    let mut consumer = Consumer::new(MemoryBackend::default())?;
    consumer.start();
    let job_id = consumer
        .schedule::<Log>(&"test, memory".to_string(), QueueName::from("default"))
        .await?;
    consumer.drain(true).await?;
    Ok(())
}
