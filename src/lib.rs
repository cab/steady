use chrono::{DateTime, Duration, Utc};
use nanoid::nanoid;
use redis::{AsyncCommands, FromRedisValue, RedisWrite, ToRedisArgs};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;
use std::{collections::VecDeque, num::NonZeroUsize};
use thiserror::private::AsDynError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;
use tokio::time;
use tracing::{debug, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait Schedulable: Serialize + DeserializeOwned {
    type Error;

    fn perform(&mut self) -> std::result::Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct Job {}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobId(String);

impl JobId {
    fn random() -> Self {
        Self(nanoid!())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JobDefinition {
    serialized_job: Vec<u8>,
    id: JobId,
    enqueued_at: DateTime<Utc>,
    #[serde(skip)]
    debug: Option<JobDefinitionDebug>,
}

#[derive(Debug)]
struct JobDefinitionDebug {
    job_type_name: &'static str,
}

impl JobDefinitionDebug {
    fn new<T>() -> Self {
        Self {
            job_type_name: std::any::type_name::<T>(),
        }
    }
}

impl FromRedisValue for JobDefinition {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let bytes = <Vec<u8> as FromRedisValue>::from_redis_value(v)?;
        let def = bincode::deserialize::<Self>(&bytes)
            .map_err(|e| (redis::ErrorKind::TypeError, "bincode failed"))?;
        Ok(def)
    }
}

impl JobDefinition {
    fn new<S>(job: &S, enqueued_at: DateTime<Utc>) -> Result<Self>
    where
        S: Serialize,
    {
        let id = JobId::random();
        let serialized_job = bincode::serialize(job)?;
        Ok(Self {
            serialized_job,
            id,
            enqueued_at,
            debug: Some(JobDefinitionDebug::new::<S>()),
        })
    }

    fn to_redis_args(&self) -> Result<impl ToRedisArgs> {
        let bytes = bincode::serialize(self)?;
        Ok(bytes)
    }
}

#[derive(Debug)]
struct Queue {
    name: QueueName,
    jobs: VecDeque<JobDefinition>,
    redis_client: redis::Client,
}

impl Queue {
    fn new(name: QueueName, redis_client: redis::Client) -> Self {
        Self {
            name,
            redis_client,
            jobs: VecDeque::new(),
        }
    }

    pub(crate) async fn process(&mut self) -> Result<()> {
        let max_pending_jobs = 3; //todo configurable
        if self.jobs.len() < max_pending_jobs {
            self.pull(NonZeroUsize::new(max_pending_jobs).unwrap())
                .await?;
        }
        self.run_next_job().await?;
        Ok(())
    }

    async fn run_next_job(&mut self) -> Result<()> {
        if let Some(next_job) = self.jobs.pop_front() {
            info!("running job: {:?}", next_job);
        }
        Ok(())
    }

    pub(crate) async fn drain(&mut self) -> Result<()> {
        info!("draining {}", self.name);
        while !self.jobs.is_empty() {
            if let Err(e) = self.run_next_job().await {
                warn!("job failed: {}", e);
            }
        }
        Ok(())
    }

    async fn pull(&mut self, count: NonZeroUsize) -> Result<()> {
        let mut connection = self.redis_client.get_async_connection().await?;
        let job_def = connection
            .rpop::<_, JobDefinition>(&self.name, Some(count))
            .await?;
        self.jobs.push_back(job_def);
        Ok(())
    }
}

mod backends {
    pub trait Backend {}
}

pub struct Scheduler<Backend> {
    redis_client: redis::Client,
    poller: Poller,
    manager: Manager,
}

impl<Backend> Scheduler<Backend> {
    pub fn new(redis_url: &str) -> Result<Self> {
        let redis_client = redis::Client::open(redis_url)?;
        let poller = Poller::new(redis_client.clone(), Duration::seconds(1));
        let manager = Manager::new(redis_client.clone(), Duration::seconds(1));
        Ok(Self {
            redis_client,
            poller,
            manager,
        })
    }

    pub fn start(&mut self) {
        self.manager.start();
        self.poller.start();
    }

    pub async fn drain(&mut self) -> Result<()> {
        self.poller.stop().await;
        self.manager.drain().await;
        Ok(())
    }

    pub async fn schedule(&self, job: impl Schedulable) -> Result<JobId> {
        let queue = QueueName::from("default");
        let job_def = JobDefinition::new(&job, chrono::Utc::now())?;
        debug!("scheduling {:?} on {}", job_def, queue);
        let mut connection = self.redis_client.get_async_connection().await?;
        let () = connection.lpush(&queue, job_def.to_redis_args()?).await?;
        Ok(job_def.id)
    }
}

#[derive(Debug)]
struct Manager {
    rate: Duration,
    job_comms: (UnboundedSender<()>, UnboundedReceiver<()>),
    handle_comms: (UnboundedSender<()>, Option<UnboundedReceiver<()>>),
    redis_client: redis::Client,
    timer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    inner: Arc<RwLock<ManagerInner>>,
}

#[derive(Debug)]
struct ManagerInner {
    queues: Vec<Queue>,
}

impl Manager {
    fn new(redis_client: redis::Client, rate: Duration) -> Self {
        let job_comms = unbounded_channel();
        let handle_comms = unbounded_channel();
        let queues = vec![Queue::new(QueueName::from("default"), redis_client.clone())];
        Self {
            timer_handle: None,
            handle_comms: (handle_comms.0, Some(handle_comms.1)),
            inner: Arc::new(RwLock::new(ManagerInner { queues })),
            rate,
            job_comms,
            redis_client,
        }
    }

    async fn drain(&mut self) -> Result<()> {
        debug!("sending message to drain manager");
        self.handle_comms.0.send(()).unwrap(); // todo handle error
        for queue in &mut self.inner.write().await.queues {
            if let Err(e) = queue.drain().await {
                warn!("failed to drain `{}`: {}", queue.name, e);
            }
        }
        if let Some(handle) = self.timer_handle.take() {
            let output = handle.await.unwrap(); // TODO handle error
            if let Err(e) = output {
                warn!("manager errored TODO");
            }
        }
        Ok(())
    }

    fn start(&mut self) {
        if self.timer_handle.is_some() {
            warn!("already started");
            return;
        }
        self.timer_handle = Some(tokio::spawn({
            let mut rx = self.handle_comms.1.take().unwrap();
            let tx = self.job_comms.0.clone();
            let rate = self.rate.clone();
            let inner = self.inner.clone();
            async move {
                let mut interval = time::interval(rate.to_std().unwrap());
                loop {
                    if let Ok(task_message) = rx.try_recv() {
                        info!("manager stopping");
                        // todo multiple types of message
                        break;
                    }
                    interval.tick().await;

                    let mut inner = inner.write().await;
                    for queue in &mut inner.queues {
                        queue.process().await; // todo handle error
                    }

                    if let Err(e) = tx.send(()) {
                        warn!("failed to send, todo");
                    }
                }
                Result::Ok(())
            }
        }));
    }
}

#[derive(Debug)]
struct Poller {
    rate: Duration,
    redis_client: redis::Client,
    job_comms: (UnboundedSender<()>, UnboundedReceiver<()>),
    handle_comms: (UnboundedSender<()>, Option<UnboundedReceiver<()>>),
    timer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl Poller {
    fn new(redis_client: redis::Client, rate: Duration) -> Self {
        let job_comms = unbounded_channel();
        let handle_comms = unbounded_channel();
        Self {
            handle_comms: (handle_comms.0, Some(handle_comms.1)),
            timer_handle: None,
            redis_client,
            job_comms,
            rate,
        }
    }

    async fn stop(&mut self) -> Result<()> {
        debug!("sending message to stop poller");
        self.handle_comms.0.send(()).unwrap(); // todo handle error
        if let Some(handle) = self.timer_handle.take() {
            let output = handle.await.unwrap(); // TODO handle error
            if let Err(e) = output {
                warn!("poller errored TODO");
            }
        }
        Ok(())
    }

    fn start(&mut self) {
        if self.timer_handle.is_some() {
            warn!("already started");
            return;
        }
        self.timer_handle = Some(tokio::spawn({
            let mut rx = self.handle_comms.1.take().unwrap();
            let tx = self.job_comms.0.clone();
            let rate = self.rate.clone();
            async move {
                let mut interval = time::interval(rate.to_std().unwrap());
                loop {
                    if let Ok(task_message) = rx.try_recv() {
                        info!("poller stopping");
                        // todo multiple types of message
                        break;
                    }
                    interval.tick().await;
                    if let Err(e) = tx.send(()) {
                        warn!("failed to send, todo");
                    }
                }
                Result::Ok(())
            }
        }));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueName(String);

impl std::fmt::Display for QueueName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("queue:{}", self.0))
    }
}

impl<S> From<S> for QueueName
where
    S: Into<String>,
{
    fn from(s: S) -> Self {
        Self(s.into())
    }
}

impl QueueName {}

impl ToRedisArgs for QueueName {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let key = format!("queue:{}", self.0);
        out.write_arg(key.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
