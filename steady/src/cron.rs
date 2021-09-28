use chrono::{DateTime, Utc};
use std::marker::PhantomData;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, info, instrument, Instrument};

use crate::{
    backends,
    jobs::{self, JobDefinition},
    producer::Producer,
    Error, JobHandler, QueueName, Result,
};

#[derive(Debug)]
struct ScheduledJobDefinition {
    job: JobDefinition,
    schedule: cron::Schedule,
    next: Option<DateTime<Utc>>,
}

impl ScheduledJobDefinition {
    fn new(job: JobDefinition, schedule: cron::Schedule) -> Self {
        Self {
            next: None,
            job,
            schedule,
        }
    }

    fn is_scheduled(&self) -> bool {
        self.next.is_some()
    }

    fn update_next(&mut self) -> DateTime<Utc> {
        let next = self.schedule.upcoming(Utc).next().unwrap();
        self.next = Some(next);
        next
    }
}

pub struct CronScheduler<Backend> {
    backend: Backend,
    scheduled_jobs: Vec<ScheduledJobDefinition>,
}

impl<Backend> std::fmt::Debug for CronScheduler<Backend> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CronScheduler")
            .field("scheduled_jobs", &self.scheduled_jobs)
            .finish()
    }
}

impl<Backend> CronScheduler<Backend>
where
    Backend: backends::CronBackend + 'static,
{
    pub fn new(backend: Backend) -> Self {
        Self {
            scheduled_jobs: Vec::new(),
            backend,
        }
    }

    #[instrument(skip(self))]
    pub async fn schedule<A>(
        &mut self,
        job_name: &str,
        job_data: &A,
        queue: QueueName,
        cron_schedule: &str,
    ) -> Result<()>
    where
        A: prost::Message,
    {
        let job_def = JobDefinition::new::<A, _>(job_data, job_name, queue, chrono::Utc::now())?;
        self.schedule_job(job_def, cron_schedule).await
    }

    #[instrument(skip(self))]
    pub async fn schedule_for_handler<T>(
        &mut self,
        job_data: &T::Arg,
        queue: QueueName,
        cron_schedule: &str,
    ) -> Result<()>
    where
        T: JobHandler,
    {
        self.schedule::<T::Arg>(T::NAME, job_data, queue, cron_schedule)
            .await
    }

    async fn schedule_job(&mut self, job: JobDefinition, cron_schedule: &str) -> Result<()> {
        let schedule = cron_schedule.parse::<cron::Schedule>()?;
        debug!("scheduling {:?} for {:?}", job, schedule);
        self.scheduled_jobs
            .push(ScheduledJobDefinition::new(job, schedule));
        Ok(())
    }

    #[instrument]
    pub async fn run(self) -> Result<()> {
        let rate = std::time::Duration::from_secs(1);
        let mut jobs = self.scheduled_jobs;
        let backend = self.backend;
        tokio::spawn(
            async move {
                let mut interval = tokio::time::interval(rate);
                loop {
                    interval.tick().await;
                    // would probably be better to use zadd and let redis tell us when it's ready
                    // right now this will schedule the same job once per instance, which is wrong
                    let now = Utc::now();
                    for job in &mut jobs {
                        if !job.is_scheduled() {
                            let next = job.update_next();
                            if let Err(e) = backend.schedule(&job.job, next).await {
                                error!("failed to schedule job: {}", e);
                            }
                        }
                    }
                }
            }
            .instrument(tracing::info_span!("run_spawn")),
        )
        .await
        .map_err::<Error, _>(|e| todo!())?;
        Ok(())
    }
}
