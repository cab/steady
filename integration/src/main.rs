use anyhow::Result;
use rjobs::{QueueName, RedisBackend, Schedulable, Scheduler};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Log {
    message: String,
}

#[rjobs::job]
impl Schedulable for Log {
    async fn perform(&mut self) -> Result<(), rjobs::Error> {
        tracing::info!("log! {}", self.message);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    const REDIS_URL: &'static str = "redis://127.0.0.1";
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
