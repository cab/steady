#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),
    #[error(transparent)]
    Serialization(#[from] bincode::Error),
    #[error("handler already registered for name '{0}'")]
    HandlerAlreadyRegistered(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;
