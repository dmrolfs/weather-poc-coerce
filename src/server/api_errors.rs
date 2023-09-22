use thiserror::Error;

#[derive(Debug, Error, ToSchema)]
pub enum ApiError {
    #[error("failed to bootstrap server API: {0}")]
    Bootstrap(#[from] ApiBootstrapError),

    #[error("call to location registrar failed: {0}")]
    Registrar(crate::model::registrar::RegistrarFailure),

    // #[error("{0}")]
    // ParseUrl(#[from] url::ParseError),
    #[error("{0}")]
    Noaa(#[from] crate::services::noaa::NoaaWeatherError),

    #[error("Invalid URL path input: {0}")]
    Path(#[from] axum::extract::rejection::PathRejection),

    // #[error("0")]
    // IO(#[from] std::io::Error),
    #[error("Invalid JSON payload: {0}")]
    Json(#[from] axum::extract::rejection::JsonRejection),

    #[error("projection failure: {0}")]
    Projection(#[from] coerce_cqrs::projection::ProjectionError),

    #[error("HTTP engine error: {0}")]
    HttpEngine(#[from] hyper::Error),

    #[error("failure during attempted database query: {source}")]
    Database { source: anyhow::Error },

    #[error("failed database operation: {0} ")]
    Sql(#[from] sqlx::Error),

    #[error("failed joining with thread: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),
}

impl From<crate::model::registrar::RegistrarFailure> for ApiError {
    fn from(failure: crate::model::registrar::RegistrarFailure) -> Self {
        Self::Registrar(failure)
    }
}

// impl From<cqrs_es::persist::PersistenceError> for ApiError {
//     fn from(error: cqrs_es::persist::PersistenceError) -> Self {
//         Self::Database { source: error.into() }
//     }
// }

#[derive(Debug, Error, ToSchema)]
pub enum ApiBootstrapError {
    #[error("failed to initialize Registrar subsystem: {0}")]
    Registrar(#[from] crate::model::registrar::RegistrarError),

    #[error("failed to initialize Location Zone subsystem: {0}")]
    LocationZone(#[from] crate::model::zone::LocationZoneError),

    #[error("failed to initialize Update Locations subsystem: {0}")]
    UpdateLocations(#[from] crate::model::update::UpdateLocationsError),

    #[error("failed to set up the journal postgres storage: {0}")]
    Journal(#[from] coerce_cqrs::postgres::PostgresStorageError),

    #[error("failed to connect with NOAA weather service: {0}")]
    Noaa(#[from] crate::services::noaa::NoaaWeatherError),

    #[error("{0}")]
    InvalidHeaderValue(#[from] axum::http::header::InvalidHeaderValue),

    #[error("{0}")]
    ParseUrl(#[from] url::ParseError),

    #[error("{0}")]
    IO(#[from] std::io::Error),
}
