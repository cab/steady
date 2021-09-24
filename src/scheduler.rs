use crate::{
    backends,
    error::Result,
    jobs::{self, JobDefinition, Schedulable},
};
use chrono::Duration;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
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
        let manager = Manager::new(backend.clone(), Duration::seconds(1));
        let poller = Poller::new(backend.clone(), Duration::seconds(1), manager.action_tx());
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

    pub async fn schedule(&self, job: impl Schedulable, queue: QueueName) -> Result<jobs::JobId> {
        let job_def = jobs::JobDefinition::new(&job, queue, chrono::Utc::now())?;
        debug!("scheduling {:?}", job_def);
        self.backend.schedule(&job_def).await?;
        Ok(job_def.id)
    }
}

#[derive(Debug)]
struct Manager<Backend> {
    rate: Duration,
    handle_comms: (
        UnboundedSender<ManagerAction>,
        Option<UnboundedReceiver<ManagerAction>>,
    ),
    backend: Backend,
    timer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
    inner: Arc<RwLock<ManagerInner<Backend>>>,
}

#[derive(Debug)]
struct ManagerInner<Backend> {
    queues_by_name: HashMap<QueueName, Queue<Backend>>,
}

#[derive(Debug)]
enum ManagerAction {
    Stop,
    PushJobs(Vec<JobDefinition>),
}

impl<Backend> Manager<Backend>
where
    Backend: backends::Backend + 'static,
{
    fn new(backend: Backend, rate: Duration) -> Self {
        let handle_comms = unbounded_channel();
        let queues_by_name = vec![Queue::new(QueueName::from("default"), backend.clone())]
            .into_iter()
            .map(|queue| (queue.name.clone(), queue))
            .collect();
        Self {
            timer_handle: None,
            handle_comms: (handle_comms.0, Some(handle_comms.1)),
            inner: Arc::new(RwLock::new(ManagerInner { queues_by_name })),
            rate,
            backend,
        }
    }

    fn action_tx(&self) -> UnboundedSender<ManagerAction> {
        self.handle_comms.0.clone()
    }

    async fn drain(&mut self, from_backend: bool) -> Result<()> {
        debug!("sending message to drain manager");
        self.handle_comms.0.send(ManagerAction::Stop).unwrap(); // todo handle error
        for queue in self.inner.write().await.queues_by_name.values_mut() {
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
            let rate = self.rate;
            let inner = self.inner.clone();
            async move {
                let mut interval = time::interval(rate.to_std().unwrap());
                loop {
                    if let Ok(action) = rx.try_recv() {
                        match action {
                            ManagerAction::Stop => {
                                info!("manager stopping");
                                break;
                            }
                            ManagerAction::PushJobs(all_jobs) => {
                                let jobs_by_queue = all_jobs
                                    .into_iter()
                                    .map(|job| (job.queue.clone(), vec![job]));
                                for (queue, jobs) in jobs_by_queue {
                                    if let Some(queue) =
                                        inner.write().await.queues_by_name.get_mut(&queue)
                                    {
                                        queue.append_jobs(jobs);
                                    }
                                }
                            }
                        }
                    }
                    interval.tick().await;

                    let mut inner = inner.write().await;
                    for queue in inner.queues_by_name.values_mut() {
                        // todo handle error
                        if let Err(e) = queue.process().await {
                            warn!("{} failed to process: {}", queue.name, e);
                        }
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
    manager_tx: UnboundedSender<ManagerAction>,
    handle_comms: (UnboundedSender<()>, Option<UnboundedReceiver<()>>),
    timer_handle: Option<tokio::task::JoinHandle<Result<()>>>,
}

// todo better name
struct PollerInner<Backend> {
    backend: Backend,
}

impl<Backend> PollerInner<Backend> {
    fn new(backend: Backend) -> Self {
        Self { backend }
    }

    async fn poll(&self) -> Result<Vec<JobDefinition>> {
        Ok(vec![])
    }
}

impl<Backend> Poller<Backend>
where
    Backend: backends::Backend + 'static,
{
    fn new(backend: Backend, rate: Duration, manager_tx: UnboundedSender<ManagerAction>) -> Self {
        let handle_comms = unbounded_channel();
        Self {
            handle_comms: (handle_comms.0, Some(handle_comms.1)),
            timer_handle: None,
            backend,
            manager_tx,
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
            let manager_tx = self.manager_tx.clone();
            let rate = self.rate;
            let inner = PollerInner::new(self.backend.clone());
            async move {
                let mut interval = time::interval(rate.to_std().unwrap());
                loop {
                    if let Ok(_task_message) = rx.try_recv() {
                        info!("poller stopping");
                        // todo multiple types of message
                        break;
                    }

                    interval.tick().await;

                    match inner.poll().await {
                        Ok(jobs) => {
                            manager_tx.send(ManagerAction::PushJobs(jobs)).unwrap();
                            // todo error handle
                        }
                        Err(e) => {
                            todo!("{}", e);
                        }
                    }
                }
                Result::Ok(())
            }
        }));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
