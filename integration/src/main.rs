use anyhow::Result;
use rjobs::{
    Consumer, CronScheduler, ErrorHandler, JobHandler, JobId, Producer, QueueName, RedisBackend,
};
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Default)]
struct Log {}

pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/integration.jobs.rs"));
}

#[rjobs::async_trait]
impl JobHandler for Log {
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

#[rjobs::async_trait]
impl JobHandler for Log2 {
    const NAME: &'static str = "log2";
    type Arg = protos::Log;
    type Error = anyhow::Error;

    async fn perform(&mut self, arg: Self::Arg) -> Result<(), Self::Error> {
        Err(anyhow::anyhow!("test failure"))
    }
}

struct JobErrorHandler;

impl ErrorHandler for JobErrorHandler {
    fn job_failed(&self, job_id: &JobId, job_name: &str, error: &rjobs::Error) {
        warn!("JOB FAILEDDDDDD");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        // .json()
        .with_max_level(tracing::Level::TRACE)
        .init();
    const REDIS_URL: &'static str = "redis://127.0.0.1";
    let backend = RedisBackend::new(REDIS_URL)?;
    let mut consumer = Consumer::new(backend.clone())?;
    consumer.add_error_handler(JobErrorHandler);
    consumer.register_handler::<Log>()?;
    consumer.register_handler::<Log2>()?;
    consumer.start();
    // let mut cron = CronScheduler::for_scheduler(&scheduler);

    let mut producer = Producer::new(backend.clone());

    let job_id = producer
        .schedule_for_handler::<Log>(
            &protos::Log {
                message: Some("hi".to_string()),
            },
            QueueName::from("default"),
        )
        .await?;
    let job_id = producer
        .schedule::<protos::Log>(
            "log2",
            &protos::Log {
                message: Some("hello".to_string()),
            },
            QueueName::from("default"),
        )
        .await?;

    let job_id = producer
        .schedule::<protos::Log>(
            "log-no-handler",
            &protos::Log {
                message: Some("hello".to_string()),
            },
            QueueName::from("default"),
        )
        .await?;

    consumer.drain(true).await?;
    Ok(())
}
