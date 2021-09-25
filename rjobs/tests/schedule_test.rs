use anyhow::Result;
use rjobs::{job, MemoryBackend, QueueName, RedisBackend, Schedulable, Scheduler};
use serde::{Deserialize, Serialize};
use test_env_log::test as logtest;

const REDIS_URL: &'static str = "redis://127.0.0.1";

#[derive(Debug, Serialize, Deserialize, Default)]
struct Log {}

#[job]
impl Schedulable for Log {
    async fn perform(&mut self) -> Result<(), rjobs::Error> {
        tracing::info!("log! {}", self.message);
        Ok(())
    }
}

#[logtest(tokio::test)]
async fn test_redis() -> Result<()> {
    let mut scheduler = Scheduler::new(RedisBackend::new(REDIS_URL)?)?;
    scheduler.start();
    let job_id = scheduler
        .schedule(
            Log {
                message: "test, redis".into(),
            },
            QueueName::from("default"),
        )
        .await?;
    scheduler.drain(true).await?;
    Ok(())
}

#[logtest(tokio::test)]
async fn test_memory() -> Result<()> {
    let mut scheduler = Scheduler::new(MemoryBackend::default())?;
    scheduler.start();
    let job_id = scheduler
        .schedule(
            Log {
                message: "test, memory".into(),
            },
            QueueName::from("default"),
        )
        .await?;
    scheduler.drain(true).await?;
    Ok(())
}
