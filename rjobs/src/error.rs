use crate::jobs::{JobDefinition, JobId};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
    #[error("handler already registered for name '{0}'")]
    HandlerAlreadyRegistered(&'static str),
    #[error("handler not found for job {0:?}")]
    NoHandler(JobDefinition),
    #[error("job {} ({}) failed: {}", .0.job_name, .0.id, .1)]
    JobFailed(JobDefinition, #[source] StdError),
    #[error("job failures: {0:?}")]
    JobFailures(Vec<Error>),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
