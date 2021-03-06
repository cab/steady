use anyhow::Result;
use steady::{
    Consumer, CronScheduler, ErrorHandler, JobHandler, JobId, Producer, QueueName, RedisBackend,
};
use tracing::warn;
use tracing_subscriber::EnvFilter;

#[derive(Default)]
struct Log {}

pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/integration.jobs.rs"));
}

#[steady::async_trait]
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

#[steady::async_trait]
impl JobHandler for Log2 {
    const NAME: &'static str = "log2";
    type Arg = protos::Log;
    type Error = anyhow::Error;

    async fn perform(&mut self, _arg: Self::Arg) -> Result<(), Self::Error> {
        Err(anyhow::anyhow!("test failure"))
    }
}

struct JobErrorHandler;

impl ErrorHandler for JobErrorHandler {
    fn job_failed(&self, job_id: &JobId, job_name: &str, error: &steady::Error) {
        warn!("JOB FAILEDDDDDD {} - {}: {}", job_id, job_name, error);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    const REDIS_URL: &str = "redis://127.0.0.1";
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        // .json()
        .init();
    let backend = RedisBackend::new(REDIS_URL)?;
    let mut consumer = Consumer::new(backend.clone())?;
    consumer.add_error_handler(JobErrorHandler);
    consumer.register_handler::<Log>()?;
    consumer.register_handler::<Log2>()?;

    let producer = Producer::new(backend.clone());
    let mut cron = CronScheduler::new(producer.clone());

    cron.schedule_for_handler::<Log>(
        &protos::Log {
            message: Some("hello from the future".into()),
        },
        QueueName::from("default"),
        "*/10 * * * * *",
    )
    .await?;

    let _job_id = producer
        .enqueue_for_handler::<Log>(
            &protos::Log {
                message: Some("hi".to_string()),
            },
            QueueName::from("default"),
            steady::Utc::now() + steady::Duration::seconds(5),
        )
        .await?;

    // let job_id = producer
    //     .enqueue::<protos::Log>(
    //         "log2",
    //         &protos::Log {
    //             message: Some("hello".to_string()),
    //         },
    //         QueueName::from("default"),
    //     )
    //     .await?;

    // let job_id = producer
    //     .enqueue::<protos::Log>(
    //         "log-no-handler",
    //         &protos::Log {
    //             message: Some("hello".to_string()),
    //         },
    //         QueueName::from("default"),
    //     )
    //     .await?;

    tokio::try_join!(cron.run(), consumer.run()).unwrap();
    Ok(())
}
