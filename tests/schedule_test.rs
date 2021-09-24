use anyhow::Result;
use rjobs::{MemoryBackend, RedisBackend, Schedulable, Scheduler};
use serde::{Deserialize, Serialize};
use test_env_log::test as logtest;

const REDIS_URL: &'static str = "redis://127.0.0.1";

#[derive(Debug, Serialize, Deserialize)]
struct Log {
    message: String,
}

impl Schedulable for Log {
    type Error = anyhow::Error;

    fn perform(&mut self) -> Result<(), Self::Error> {
        tracing::info!("log! {}", self.message);
        Ok(())
    }
}

#[logtest(tokio::test)]
async fn test_add() -> Result<()> {
    let mut scheduler = Scheduler::new(RedisBackend::new(REDIS_URL)?)?;
    scheduler.start();
    let job_id = scheduler
        .schedule(Log {
            message: "test".into(),
        })
        .await?;
    scheduler.drain(true).await?;
    Ok(())
}

#[logtest(tokio::test)]
async fn test_memory() -> Result<()> {
    let mut scheduler = Scheduler::new(MemoryBackend::default())?;
    scheduler.start();
    let job_id = scheduler
        .schedule(Log {
            message: "test".into(),
        })
        .await?;
    scheduler.drain(true).await?;
    Ok(())
}
