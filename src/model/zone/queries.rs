use crate::model::zone::LocationZoneEvent;
use crate::model::{ForecastDetail, WeatherAlert, WeatherFrame};
use coerce_cqrs::postgres::{PostgresProjectionStorage, PostgresStorageConfig, TableName};
use coerce_cqrs::projection::processor::ProcessResult;
use coerce_cqrs::projection::{PersistenceId, ProjectionError};
use iso8601_timestamp::Timestamp;
use once_cell::sync::Lazy;
use std::sync::Arc;

pub const ZONE_WEATHER_VIEW: &str = "zone_weather";
pub static ZONE_WEATHER_TABLE: Lazy<TableName> =
    Lazy::new(|| TableName::new(ZONE_WEATHER_VIEW).unwrap());
pub static ZONE_OFFSET_TABLE: Lazy<TableName> =
    Lazy::new(PostgresStorageConfig::default_projection_offsets_table);

pub type WeatherProjection = Arc<PostgresProjectionStorage<WeatherView>>;

#[derive(Debug, Clone, PartialEq, ToSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WeatherView {
    pub zone_code: String,

    pub timestamp: Timestamp,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alert: Option<WeatherAlert>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<WeatherFrame>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub forecast: Vec<ForecastDetail>,
}

impl Default for WeatherView {
    fn default() -> Self {
        Self {
            zone_code: String::default(),
            timestamp: Timestamp::now_utc(),
            alert: None,
            current: None,
            forecast: Vec::default(),
        }
    }
}

impl WeatherView {
    pub fn new(zone_code: impl Into<String>) -> Self {
        Self { zone_code: zone_code.into(), ..Default::default() }
    }

    pub fn apply_event(
        _persistence_id: &PersistenceId, view: &WeatherView, event: LocationZoneEvent,
    ) -> ProcessResult<Self, ProjectionError> {
        let mut result = view.clone();

        match event {
            LocationZoneEvent::Subscribed(zone) => {
                result.zone_code = zone.to_string();
            },

            LocationZoneEvent::ObservationAdded(frame) => {
                result.current = Some(*frame);
            },

            LocationZoneEvent::ForecastUpdated(forecast) => {
                result.forecast = forecast.periods;
            },

            LocationZoneEvent::AlertActivated(alert) => {
                result.alert = Some(alert);
            },

            LocationZoneEvent::AlertDeactivated => {
                result.alert = None;
            },
        }

        ProcessResult::Changed(result)
    }
}
