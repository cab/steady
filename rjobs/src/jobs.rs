use crate::{
    error::{Error, Result},
    scheduler::QueueName,
};
use chrono::{DateTime, Utc};
use nanoid::nanoid;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[rjobs_typetag::serde(tag = "job_type")]
#[async_trait::async_trait]
pub trait Schedulable: Send + Sync {
    // : Serialize + DeserializeOwned {
    async fn perform(&mut self) -> std::result::Result<(), Error>;
}

#[derive(Debug)]
pub struct Job {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobId(String);

impl JobId {
    fn random() -> Self {
        Self(nanoid!())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct JobDefinition {
    pub(crate) id: JobId,
    pub(crate) serialized_job: Vec<u8>,
    enqueued_at: DateTime<Utc>,
    pub(crate) queue: QueueName,
    #[serde(skip)]
    debug: Option<JobDefinitionDebug>,
}

impl std::fmt::Debug for JobDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobDefinition")
            .field("id", &self.id)
            .field("enqueued_at", &self.enqueued_at)
            .field("debug", &self.debug)
            .finish()
    }
}

#[derive(Debug, Clone)]
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

impl JobDefinition {
    pub(crate) fn new<S>(
        job: &dyn Schedulable,
        queue: QueueName,
        enqueued_at: DateTime<Utc>,
    ) -> Result<Self> {
        let id = JobId::random();
        let serialized_job = bincode::serialize(job)?;
        Ok(Self {
            serialized_job,
            queue,
            id,
            enqueued_at,
            debug: Some(JobDefinitionDebug::new::<S>()),
        })
    }
}
