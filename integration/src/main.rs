use anyhow::Result;
use rjobs::{QueueName, RedisBackend, Schedulable, Scheduler};
use serde::{Deserialize, Serialize};

#[derive(Default)]
struct Log {}

pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/integration.jobs.rs"));
}

#[rjobs::job]
impl Schedulable for Log {
    const NAME: &'static str = "log";
    type Arg = protos::Log;
    type Error = anyhow::Error;

    async fn perform(&mut self, arg: Self::Arg) -> Result<(), Self::Error> {
        tracing::info!("log! {:?}", arg.message);
        Ok(())
    }
}

#[derive(Default)]
struct Log2 {}

#[rjobs::job]
impl Schedulable for Log2 {
    const NAME: &'static str = "log";
    type Arg = protos::Log;
    type Error = anyhow::Error;

    async fn perform(&mut self, arg: Self::Arg) -> Result<(), Self::Error> {
        tracing::info!("log! {:?}", arg.message);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    const REDIS_URL: &'static str = "redis://127.0.0.1";
    let mut scheduler = Scheduler::new(RedisBackend::new(REDIS_URL)?)?;
    scheduler.register::<Log>()?;
    scheduler.register::<Log2>()?;
    scheduler.start();
    let job_id = scheduler
        .schedule::<Log, _>(
            &protos::Log {
                message: Some("hi".to_string()),
            },
            QueueName::from("default"),
        )
        .await?;
    scheduler.drain(true).await?;
    Ok(())
}
