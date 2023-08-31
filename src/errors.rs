use thiserror::Error;

#[derive(Debug, ToSchema, Error)]
#[non_exhaustive]
pub enum WeatherError {
    #[error("failed to convert GeoJson Feature: {0}")]
    GeoJson(#[from] geojson::Error),

    #[error("{target} expected missing GeoJson Feature property {property}")]
    MissingGeoJsonProperty { target: String, property: String },

    #[error("empty quantitative aggregation")]
    EmptyAggregation,

    #[error("missing GeoJson Feature:{0}")]
    MissingFeature(String),

    #[error("failed to parse Json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("cannot extract location zone identifier from URL: {0}")]
    UrlNotZoneIdentifier(url::Url),

    // Api(#[from] server::ApiError),
    #[error("Encountered a technical failure: {source}")]
    Unexpected { source: anyhow::Error },
}
