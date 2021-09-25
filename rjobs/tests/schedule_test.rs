use anyhow::Result;
use rjobs::{JobHandler, MemoryBackend, QueueName, RedisBackend, Scheduler};
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
    let mut scheduler = Scheduler::new(RedisBackend::new(REDIS_URL)?)?;
    scheduler.start();
    let job_id = scheduler
        .schedule::<Log>(&"test, redis".to_string(), QueueName::from("default"))
        .await?;
    scheduler.drain(true).await?;
    Ok(())
}

#[logtest(tokio::test)]
async fn test_memory() -> Result<()> {
    let mut scheduler = Scheduler::new(MemoryBackend::default())?;
    scheduler.start();
    let job_id = scheduler
        .schedule::<Log>(&"test, memory".to_string(), QueueName::from("default"))
        .await?;
    scheduler.drain(true).await?;
    Ok(())
}
