use crate::{
    backends,
    error::{ErrorHandler, ErrorHandlers, Result, StdError},
    jobs::{self, JobDefinition, JobHandler, JobId},
    Error, QueueName,
};
use chrono::Duration;
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{any::Any, collections::HashMap, sync::Arc};
use std::{collections::VecDeque, num::NonZeroUsize};
use tokio::time;
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
};
use tracing::{debug, error, info, instrument, trace, warn, Instrument};

struct Queue<Backend> {
    handlers: Handlers,
    error_handlers: ErrorHandlers,
    name: QueueName,
    manager_tx: UnboundedSender<ManagerAction>,
    pulled_jobs: VecDeque<jobs::JobDefinition>,
    backend: Backend,
}

impl<Backend> std::fmt::Debug for Queue<Backend> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue")
            .field("name", &self.name)
            .field("pulled_jobs", &self.pulled_jobs)
            .field("handlers", &self.handlers)
            .finish()
    }
}

impl<Backend> Queue<Backend>
where
    Backend: backends::Backend,
{
    fn new(
        name: QueueName,
        backend: Backend,
        handlers: Handlers,
        error_handlers: ErrorHandlers,
        manager_tx: UnboundedSender<ManagerAction>,
    ) -> Self {
        Self {
            pulled_jobs: VecDeque::new(),
            manager_tx,
            error_handlers,
            name,
            backend,
            handlers,
        }
    }

    #[instrument(name = "process_queue", skip(self), fields(queue_name = %self.name))]
    pub(crate) async fn process(&mut self) -> Result<()> {
        let max_pending_jobs = 3; //todo configurable
        if self.pulled_jobs.len() < max_pending_jobs {
            self.pull(NonZeroUsize::new(max_pending_jobs).unwrap())
                .await?;
        }
        self.run_pulled_jobs().await?;
        Ok(())
    }

    #[instrument(skip(self), fields(pulled_jobs = ?self.pulled_jobs))]
    async fn run_pulled_jobs(&mut self) -> Result<()> {
        let handlers = &self.handlers.clone();
        let jobs = std::mem::take(&mut self.pulled_jobs);
        let errors = futures::stream::iter(jobs)
            .then(|next_job| {
                // TODO can we use this in `instrument` without cloning?
                let job = next_job.clone();
                async move {
                    match handlers.get(&next_job.job_name) {
                        Some(mut handler) => handler.perform(&next_job).await,
                        None => Err(Error::NoHandler(next_job)),
                    }
                }
                .inspect_err({
                    let job = job.clone();
                    let error_handlers = self.error_handlers.clone();
                    move |e| {
                        error_handlers.job_failed(&job.id, &job.job_name, e);
                    }
                })
                .instrument(tracing::info_span!("run_pulled_job", job = ?job))
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .partition::<Vec<_>, _>(Result::is_ok)
            .1 // errors
            .into_iter()
            .map(Result::unwrap_err)
            .collect::<Vec<_>>();

        if !errors.is_empty() {
            return Err(Error::JobFailures(errors));
        }

        Ok(())
    }

    #[instrument(name = "drain_queue", skip(self), fields(queue_name = %self.name))]
    pub(crate) async fn drain(&mut self, from_backend: bool) -> Result<()> {
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
        self.run_pulled_jobs().await?;
        Ok(())
    }

    fn append_jobs(&mut self, jobs: impl IntoIterator<Item = JobDefinition>) {
        self.pulled_jobs.extend(jobs);
    }

    #[instrument(skip(self))]
    async fn pull(&mut self, limit: NonZeroUsize) -> Result<usize> {
        let job_defs = self.backend.pull(&self.name, limit).await?;
        let count = job_defs.len();
        trace!("pulled {} job{}", count, if count == 1 { "" } else { "s" });
        if count > 0 {
            self.append_jobs(job_defs);
        }
        Ok(count)
    }
}

pub struct Consumer<Backend> {
    backend: Backend,
    poller: Poller<Backend>,
    manager: Manager<Backend>,
    error_handlers: ErrorHandlers,
}

impl<Backend> std::fmt::Debug for Consumer<Backend> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler")
            .field("poller", &self.poller)
            .field("manager", &self.manager)
            .finish()
    }
}

impl<Backend> Consumer<Backend>
where
    Backend: backends::Backend + 'static,
{
    pub fn new(backend: Backend) -> Result<Self> {
        let error_handlers = ErrorHandlers::default();
        let manager = Manager::new(
            backend.clone(),
            Duration::milliseconds(250),
            error_handlers.clone(),
        );
        let poller = Poller::new(
            backend.clone(),
            Duration::seconds(1),
            manager.action_tx(),
            error_handlers.clone(),
        );
        Ok(Self {
            error_handlers,
            backend,
            poller,
            manager,
        })
    }

    pub fn add_error_handler(&mut self, handler: impl ErrorHandler + 'static) {
        self.error_handlers.add(handler);
    }

    pub fn register_handler<S>(&mut self) -> Result<()>
    where
        S: JobHandler + 'static,
    {
        self.manager.register_handler::<S>()
    }

    pub(crate) fn action_tx(&self) -> UnboundedSender<ManagerAction> {
        self.manager.action_tx()
    }

    pub async fn run(mut self) -> Result<()> {
        let manager = self.manager;
        let poller = self.poller;
        tokio::select! {
            manager = manager.run() => {},
            poller = poller.run() => {}
        };
        Ok(())
    }
}

struct Manager<Backend> {
    rate: Duration,
    handle_comms: (
        UnboundedSender<ManagerAction>,
        UnboundedReceiver<ManagerAction>,
    ),
    backend: Backend,
    handlers: Handlers,
    error_handlers: ErrorHandlers,
}

impl<Backend> std::fmt::Debug for Manager<Backend> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager")
            .field("rate", &self.rate)
            .field("handlers", &self.handlers)
            .finish()
    }
}

#[async_trait::async_trait]
trait AnyJobHandler: Send + Sync {
    async fn perform(&mut self, job: &JobDefinition) -> Result<()>;
}

#[async_trait::async_trait]
impl<T, A, E> AnyJobHandler for T
where
    A: prost::Message + Default,
    E: Into<StdError> + Send + Sync,
    T: JobHandler<Arg = A, Error = E> + 'static,
{
    #[instrument(skip(self))]
    async fn perform(&mut self, job: &JobDefinition) -> Result<()> {
        let proto = A::decode(job.serialized_job_data.as_slice()).unwrap(); // todo
        let mut runner = T::default();
        T::perform(&mut runner, proto)
            .await
            .map_err(|e| Error::JobFailed(job.clone(), e.into()))
    }
}

#[derive(Default, Clone)]
struct Handlers {
    handlers_by_name: HashMap<String, Arc<Box<dyn Fn() -> Box<dyn AnyJobHandler> + Send + Sync>>>,
}

impl std::fmt::Debug for Handlers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handlers")
            .field("names", &self.handlers_by_name.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Handlers {
    fn register<S>(&mut self) -> Result<()>
    where
        S: JobHandler + 'static,
    {
        if self.handlers_by_name.contains_key(S::NAME) {
            let err = Error::HandlerAlreadyRegistered(S::NAME);
            warn!("{}", err);
            return Err(err);
        }
        self.handlers_by_name.insert(
            S::NAME.to_string(),
            Arc::new(Box::new(|| Box::new(S::default()))),
        );
        Ok(())
    }

    fn get(&self, name: &str) -> Option<Box<dyn AnyJobHandler>> {
        self.handlers_by_name.get(name).map(|f| f())
    }
}

#[derive(Debug)]
pub(crate) enum ManagerAction {
    Stop,
    PushJobs(Vec<JobDefinition>),
}

impl<Backend> Manager<Backend>
where
    Backend: backends::Backend + 'static,
{
    fn new(backend: Backend, rate: Duration, error_handlers: ErrorHandlers) -> Self {
        let handle_comms = unbounded_channel();
        Self {
            handle_comms: (handle_comms.0, handle_comms.1),
            handlers: Handlers::default(),
            rate,
            error_handlers,
            backend,
        }
    }

    pub fn register_handler<S>(&mut self) -> Result<()>
    where
        S: JobHandler + 'static,
    {
        self.handlers.register::<S>()?;
        Ok(())
    }

    fn action_tx(&self) -> UnboundedSender<ManagerAction> {
        self.handle_comms.0.clone()
    }

    #[instrument]
    async fn run(mut self) -> Result<()> {
        // TODO configure which queues are processed
        let queues_by_name = vec![Queue::new(
            QueueName::from("default"),
            self.backend.clone(),
            self.handlers.clone(),
            self.error_handlers.clone(),
            self.action_tx(),
        )]
        .into_iter()
        .map(|queue| (queue.name.clone(), Arc::new(Mutex::new(queue))))
        .collect::<HashMap<_, _>>();

        for (_, queue) in &queues_by_name {
            let mut interval = time::interval(self.rate.to_std().unwrap());
            let handle = tokio::spawn({
                let queue = queue.clone();
                async move {
                    loop {
                        interval.tick().await;
                        let mut queue = queue.lock().await;
                        if let Err(e) = queue.process().await {
                            error!("queue [{}] failed to process: {}", queue.name, e);
                        }
                    }
                }
            });
        }

        tokio::spawn({
            let mut rx = self.handle_comms.1;
            let rate = self.rate;

            async move {
                let mut interval = time::interval(rate.to_std().unwrap());
                loop {
                    if let Ok(action) = rx.try_recv() {
                        debug!("manager received action: {:?}", action);
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
                                    if let Some(queue) = queues_by_name.get(&queue) {
                                        // todo handle error
                                        queue.lock().await.append_jobs(jobs);
                                    }
                                }
                            }
                        }
                    }
                    interval.tick().await;
                }
                Result::Ok(())
            }
            .instrument(tracing::info_span!("manager_loop_task"))
        })
        .await
        .map_err::<Error, _>(|e| todo!())??;
        Ok(())
    }
}

struct Poller<Backend> {
    rate: Duration,
    backend: Backend,
    manager_tx: UnboundedSender<ManagerAction>,
    handle_comms: (UnboundedSender<()>, UnboundedReceiver<()>),
    error_handlers: ErrorHandlers,
}

impl<Backend> std::fmt::Debug for Poller<Backend> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Poller").field("rate", &self.rate).finish()
    }
}

impl<Backend> Poller<Backend>
where
    Backend: backends::Backend + 'static,
{
    fn new(
        backend: Backend,
        rate: Duration,
        manager_tx: UnboundedSender<ManagerAction>,
        error_handlers: ErrorHandlers,
    ) -> Self {
        let handle_comms = unbounded_channel();
        Self {
            handle_comms,
            error_handlers,
            backend,
            manager_tx,
            rate,
        }
    }

    #[instrument]
    async fn run(self) -> Result<()> {
        tokio::spawn({
            let mut rx = self.handle_comms.1;
            let manager_tx = self.manager_tx.clone();
            let rate = self.rate;
            let backend = self.backend;
            let pull_count = NonZeroUsize::new(100).unwrap();
            async move {
                let mut interval = time::interval(rate.to_std().unwrap());
                loop {
                    if let Ok(_task_message) = rx.try_recv() {
                        info!("poller stopping");
                        // todo multiple types of message
                        break;
                    }

                    interval.tick().await;

                    match backend.pull_scheduled(pull_count).await {
                        Ok(jobs) => {
                            if !jobs.is_empty() {
                                manager_tx.send(ManagerAction::PushJobs(jobs)).unwrap();
                                // todo error handle
                            }
                        }
                        Err(e) => {
                            error!("TODO {}", e);
                        }
                    }
                }
                Result::Ok(())
            }
            .instrument(tracing::info_span!("poller_loop_task"))
        })
        .await
        .map_err::<Error, _>(|e| {
            warn!("unhandled: {}", e);
            todo!()
        })??;
        Ok(())
    }
}
