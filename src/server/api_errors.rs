use thiserror::Error;

#[derive(Debug, Error, ToSchema)]
pub enum ApiError {
    #[error("{0}")]
    ParseUrl(#[from] url::ParseError),

    // #[error("{0}")]
    // Noaa(#[from] crate::services::noaa::NoaaWeatherError),
    #[error("Invalid URL path input: {0}")]
    Path(#[from] axum::extract::rejection::PathRejection),

    #[error("0")]
    IO(#[from] std::io::Error),

    #[error("Invalid JSON payload: {0}")]
    Json(#[from] axum::extract::rejection::JsonRejection),

    // #[error("call to location registrar failed: {0}")]
    // Registrar(#[from] cqrs_es::AggregateError<RegistrarError>),
    #[error("HTTP engine error: {0}")]
    HttpEngine(#[from] hyper::Error),

    #[error("failure during attempted database query: {source}")]
    Database { source: anyhow::Error },

    #[error("failed database operation: {0} ")]
    Sql(#[from] sqlx::Error),

    #[error("failed joining with thread: {0}")]
    Join(#[from] tokio::task::JoinError),
}

// impl From<cqrs_es::persist::PersistenceError> for ApiError {
//     fn from(error: cqrs_es::persist::PersistenceError) -> Self {
//         Self::Database { source: error.into() }
//     }
// }
