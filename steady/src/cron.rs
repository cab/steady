use chrono::{DateTime, Duration, Utc};
use std::marker::PhantomData;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, info, instrument, trace, warn, Instrument};

use crate::{
    backends,
    jobs::{self, JobDefinition},
    producer::Producer,
    Error, JobHandler, QueueName, Result,
};

struct ScheduledJobDefinition {
    job: JobDefinition,
    schedule: cron::Schedule,
    next_time: Option<DateTime<Utc>>,
}

impl std::fmt::Debug for ScheduledJobDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScheduledJobDefinition")
            .field("job", &self.job)
            .field("schedule", &self.schedule)
            .finish()
    }
}

impl ScheduledJobDefinition {
    fn new(job: JobDefinition, schedule: cron::Schedule) -> Self {
        Self {
            next_time: None,
            job,
            schedule,
        }
    }

    fn set_next_time(&mut self, next: DateTime<Utc>) {
        self.next_time = Some(next);
    }

    fn next(&self) -> DateTime<Utc> {
        if let Some(last_run) = self.next_time.as_ref() {
            self.schedule.after(last_run).next().unwrap()
        } else {
            self.schedule.upcoming(Utc).next().unwrap()
        }
    }
}

pub struct CronScheduler<Backend> {
    producer: Producer<Backend>,
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
    Backend: backends::Backend + 'static,
{
    pub fn new(producer: Producer<Backend>) -> Self {
        Self {
            scheduled_jobs: Vec::new(),
            producer,
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
        let producer = self.producer;

        for mut job in jobs {
            let span = tracing::info_span!("scheduled_job", job = ?job);
            let handle = tokio::spawn(
                {
                    let producer = producer.clone();
                    async move {
                        loop {
                            let next = job.next();
                            let now = Utc::now();
                            if next <= now {
                                if let Err(e) =
                                    producer.enqueue_job(&job.job.with_new_id(), now).await
                                {
                                    // todo
                                    error!("failed to enqueue: {}", e);
                                }
                            } else {
                                if let Err(e) =
                                    producer.enqueue_job(&job.job.with_new_id(), next).await
                                {
                                    // todo
                                    error!("failed to enqueue: {}", e);
                                }
                                job.set_next_time(next);
                                let delta = wait_time(now, next);
                                trace!("sleeping for {:?}", delta);
                                tokio::time::sleep(delta.to_std().unwrap()).await;
                            }
                        }
                    }
                }
                .instrument(span),
            );
        }

        Ok(())
    }
}

fn wait_time(now: DateTime<Utc>, next: DateTime<Utc>) -> Duration {
    let mut delta = next - now;
    if delta > Duration::seconds(10) {
        delta = delta - Duration::seconds(5);
    }
    delta
}
