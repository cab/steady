use crate::jobs::JobId;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
    #[error("handler already registered for name '{0}'")]
    HandlerAlreadyRegistered(&'static str),
    #[error("handler not found for name '{1}' when running job {0}")]
    NoHandler(JobId, String),
    #[error("job failed: {0}")]
    JobFailed(#[source] StdError),
    #[error("job failures: {0:?}")]
    JobFailures(Vec<Error>),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;
