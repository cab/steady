use crate::error::{Error, Result, StdError};
use chrono::{DateTime, Utc};
use nanoid::nanoid;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[async_trait::async_trait]
pub trait JobHandler: Send + Sync + Default {
    const NAME: &'static str;
    type Arg: prost::Message + Default;
    type Error: Into<StdError> + Send + Sync;
    // : Serialize + DeserializeOwned {
    async fn perform(&mut self, arg: Self::Arg) -> std::result::Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct Job {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobId(String);

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0))
    }
}

impl JobId {
    fn random() -> Self {
        Self(nanoid!())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct JobDefinition {
    pub(crate) id: JobId,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) serialized_job_data: Vec<u8>,
    pub(crate) job_name: String,
    enqueued_at: DateTime<Utc>,
    pub(crate) queue: QueueName,
}

impl std::fmt::Debug for JobDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobDefinition")
            .field("idempotency_key", &self.idempotency_key)
            .field("id", &self.id)
            .field("job_name", &self.job_name)
            .field("queue", &self.queue)
            .field("enqueued_at", &self.enqueued_at)
            .finish()
    }
}

impl JobDefinition {
    pub(crate) fn new<S, N>(
        idempotency_key: Option<String>,
        job_data: &S,
        job_name: N,
        queue: QueueName,
        enqueued_at: DateTime<Utc>,
    ) -> Self
    where
        S: prost::Message,
        N: Into<String>,
    {
        let id = JobId::random();
        let serialized_job_data = job_data.encode_to_vec();
        Self {
            job_name: job_name.into(),
            idempotency_key,
            serialized_job_data,
            queue,
            id,
            enqueued_at,
        }
    }

    pub fn with_new_id(&self) -> Self {
        Self {
            id: JobId::random(),
            ..self.clone()
        }
    }

    pub fn with_new_idempotency_key<I>(&self, idempotency_key: Option<String>) -> Self {
        Self {
            idempotency_key,
            ..self.clone()
        }
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
        f.write_fmt(format_args!("{}", self.0))
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
