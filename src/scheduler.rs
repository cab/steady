use crate::{
    backends,
    error::Result,
    jobs::{self, JobDefinition, Schedulable},
};
use chrono::Duration;
use std::sync::Arc;
use std::{collections::VecDeque, num::NonZeroUsize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;
use tokio::time;
use tracing::{debug, error, info, warn};

#[derive(Debug)]
struct Queue<Backend> {
    name: QueueName,
    jobs: VecDeque<jobs::JobDefinition>,
    backend: Backend,
}

impl<Backend> Queue<Backend>
where
    Backend: backends::Backend,
{
    fn new(name: QueueName, backend: Backend) -> Self {
        Self {
            name,
            backend,
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

    pub(crate) async fn drain(&mut self, from_backend: bool) -> Result<()> {
        info!("draining {}", self.name);
        if from_backend {
            loop {
                match self.pull(NonZeroUsize::new(100).unwrap()).await {
                    Err(e) => {
                        warn!("failed to drain from backend: {}", e);
                        break;
                    }
                    Ok(size) => {
                        if size == 0 {
                            break;
                        }
                    }
                }
            }
        }
        while !self.jobs.is_empty() {
            if let Err(e) = self.run_next_job().await {
                warn!("job failed: {}", e);
            }
        }
        Ok(())
    }

    fn append_jobs(&mut self, jobs: impl IntoIterator<Item = JobDefinition>) {
        self.jobs.extend(jobs);
    }

    async fn pull(&mut self, count: NonZeroUsize) -> Result<usize> {
        let job_defs = self.backend.pull(&self.name, count).await?;
        let count = job_defs.len();
        debug!("pulled {} jobs", count);
        self.append_jobs(job_defs);
        Ok(count)
    }
}

pub struct Scheduler<Backend> {
    backend: Backend,
    poller: Poller<Backend>,
    manager: Manager<Backend>,
}

impl<Backend> Scheduler<Backend>
where
    Backend: backends::Backend + 'static,
{
    pub fn new(backend: Backend) -> Result<Self> {
        let poller = Poller::new(backend.clone(), Duration::seconds(1));
        let manager = Manager::new(backend.clone(), Duration::seconds(1));
        Ok(Self {
            backend,
            poller,
            manager,
        })
    }

    pub fn start(&mut self) {
        self.manager.start();
        self.poller.start();
    }

    pub async fn drain(&mut self, from_backend: bool) -> Result<()> {
        if let Err(e) = self.poller.stop().await {
            error!("failed to stop poller: {}", e);
        }
        if let Err(e) = self.manager.drain(from_backend).await {
            error!("failed to stop manager: {}", e);
        }
        Ok(())
    }

    pub async fn schedule(&self, job: impl Schedulable) -> Result<jobs::JobId> {
        let queue = QueueName::from("default");
        let job_def = jobs::JobDefinition::new(&job, chrono::Utc::now())?;
        debug!("scheduling {:?} on {}", job_def, queue);
        self.backend.schedule(&queue, &job_def).await?;
        Ok(job_def.id)
    }
}

#[derive(Debug)]
struct Manager<Backend> {
    rate: Duration,
    job_comms: (UnboundedSender<()>, UnboundedReceiver<()>),
    handle_comms: (UnboundedSender<()>, Option<UnboundedReceiver<()>>),
    backend: Backend,
    timer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    inner: Arc<RwLock<ManagerInner<Backend>>>,
}

#[derive(Debug)]
struct ManagerInner<Backend> {
    queues: Vec<Queue<Backend>>,
}

impl<Backend> Manager<Backend>
where
    Backend: backends::Backend + 'static,
{
    fn new(backend: Backend, rate: Duration) -> Self {
        let job_comms = unbounded_channel();
        let handle_comms = unbounded_channel();
        let queues = vec![Queue::new(QueueName::from("default"), backend.clone())];
        Self {
            timer_handle: None,
            handle_comms: (handle_comms.0, Some(handle_comms.1)),
            inner: Arc::new(RwLock::new(ManagerInner { queues })),
            rate,
            job_comms,
            backend,
        }
    }

    async fn drain(&mut self, from_backend: bool) -> Result<()> {
        debug!("sending message to drain manager");
        self.handle_comms.0.send(()).unwrap(); // todo handle error
        for queue in &mut self.inner.write().await.queues {
            if let Err(e) = queue.drain(from_backend).await {
                warn!("failed to drain `{}`: {}", queue.name, e);
            }
        }
        if let Some(handle) = self.timer_handle.take() {
            let output = handle.await.unwrap(); // TODO handle error
            if let Err(e) = output {
                warn!("manager errored: {}", e); // todo
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
            let rate = self.rate;
            let inner = self.inner.clone();
            async move {
                let mut interval = time::interval(rate.to_std().unwrap());
                loop {
                    if let Ok(_task_message) = rx.try_recv() {
                        info!("manager stopping");
                        // todo multiple types of message
                        break;
                    }
                    interval.tick().await;

                    let mut inner = inner.write().await;
                    for queue in &mut inner.queues {
                        // todo handle error
                        if let Err(e) = queue.process().await {
                            warn!("{} failed to process: {}", queue.name, e);
                        }
                    }

                    if let Err(e) = tx.send(()) {
                        // todo
                        warn!("failed to send: {}", e);
                    }
                }
                Result::Ok(())
            }
        }));
    }
}

#[derive(Debug)]
struct Poller<Backend> {
    rate: Duration,
    backend: Backend,
    job_comms: (UnboundedSender<()>, UnboundedReceiver<()>),
    handle_comms: (UnboundedSender<()>, Option<UnboundedReceiver<()>>),
    timer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl<Backend> Poller<Backend>
where
    Backend: backends::Backend + 'static,
{
    fn new(backend: Backend, rate: Duration) -> Self {
        let job_comms = unbounded_channel();
        let handle_comms = unbounded_channel();
        Self {
            handle_comms: (handle_comms.0, Some(handle_comms.1)),
            timer_handle: None,
            backend,
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
                // todo
                warn!("poller errored: {}", e);
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
            let rate = self.rate;
            async move {
                let mut interval = time::interval(rate.to_std().unwrap());
                loop {
                    if let Ok(_task_message) = rx.try_recv() {
                        info!("poller stopping");
                        // todo multiple types of message
                        break;
                    }
                    interval.tick().await;
                    if let Err(e) = tx.send(()) {
                        // todo
                        warn!("failed to send: {}", e);
                    }
                }
                Result::Ok(())
            }
        }));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueName(String);

impl QueueName {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

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
