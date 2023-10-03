use crate::model::zone::LocationZoneEvent;
use crate::model::{ForecastDetail, WeatherAlert, WeatherFrame};
use coerce_cqrs::postgres::{
    BinaryProjection, PostgresProjectionStorage, PostgresStorageConfig, TableName,
};
use coerce_cqrs::projection::processor::ProcessResult;
use coerce_cqrs::projection::{PersistenceId, ProjectionError};
use iso8601_timestamp::Timestamp;
use once_cell::sync::Lazy;
use serde::Serialize;
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
        persistence_id: &PersistenceId, view: &WeatherView, event: LocationZoneEvent,
    ) -> ProcessResult<Self, ProjectionError> {
        let mut result = view.clone();

        match event {
            LocationZoneEvent::Started => {
                result.zone_code = persistence_id.id.to_string();
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

impl BinaryProjection for WeatherView {
    type BinaryCodecError = pot::Error;

    fn as_bytes(&self) -> Result<Vec<u8>, Self::BinaryCodecError> {
        pot::to_vec(self)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::BinaryCodecError> {
        pot::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use crate::model::zone::WeatherView;
    use claim::*;
    use coerce_cqrs::postgres::BinaryProjection;
    use coerce_cqrs::projection::ProjectionError;
    use pretty_assertions::assert_eq;

    fn as_bytes<T: BinaryProjection>(value: &T) -> Result<Vec<u8>, ProjectionError> {
        value.as_bytes().map_err(|err| ProjectionError::Encode(err.into()))
    }

    fn from_bytes<T: BinaryProjection>(bytes: &[u8]) -> Result<T, ProjectionError> {
        T::from_bytes(bytes).map_err(|err| {
            error!(error=?err, "DMR: DECODE ERROR");
            ProjectionError::Decode(err.into())
        })
    }

    #[test]
    fn test_default_weather_view_serde() {
        // once_cell::sync::Lazy::force(&coerce_cqrs_test::setup_tracing::TEST_TRACING);
        // let main_span = info_span!("test_default_weather_view_serde");
        // let _main_span_guard = main_span.enter();

        let view = WeatherView::default();
        let ts = view.timestamp.to_string();

        let view_json_ser = assert_ok!(serde_json::to_string(&view));
        assert_eq!(
            view_json_ser,
            format!("{{\"zoneCode\":\"\",\"timestamp\":\"{ts}\"}}")
        );

        let view_bin_ser = assert_ok!(as_bytes(&view));
        assert_eq!(view_bin_ser.len(), 34);

        let view_json_deser: WeatherView = assert_ok!(serde_json::from_str(&view_json_ser));
        assert_eq!(view_json_deser.zone_code, view.zone_code);
        assert_eq!(
            view_json_deser.timestamp.to_string(),
            view.timestamp.to_string()
        );
        assert_eq!(view_json_deser.alert, view.alert);
        assert_eq!(view_json_deser.current, view.current);
        assert_eq!(view_json_deser.forecast, view.forecast);

        let view_bin_deser: WeatherView = assert_ok!(from_bytes(view_bin_ser.as_slice()));
        assert_eq!(view_bin_deser, view_json_deser);
    }
}
