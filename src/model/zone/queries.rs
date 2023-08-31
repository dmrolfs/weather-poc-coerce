use coerce_cqrs::projection::processor::ProcessResult;
use coerce_cqrs::projection::ProjectionError;
use iso8601_timestamp::Timestamp;
use crate::model::{ForecastDetail, WeatherAlert, WeatherFrame};
use crate::model::zone::LocationZoneEvent;

pub const WEATHER_QUERY_VIEW: &str = "weather_query";

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
        Self {
            zone_code: zone_code.into(),
            ..Default::default()
        }
    }

    pub fn apply_event(view: &WeatherView, event: &LocationZoneEvent) -> Result<ProcessResult<Self>, ProjectionError> {
        let mut result = view.clone();

        match event {
            LocationZoneEvent::Subscribed(zone) => {
                result.zone_code = zone.to_string();
            },

            LocationZoneEvent::ObservationAdded(frame) => {
                result.current = Some(*frame.clone());
            },

            LocationZoneEvent::ForecastUpdated(forecast) => {
                result.forecast = forecast.periods.clone();
            },

            LocationZoneEvent::AlertActivated(alert) => {
                result.alert = Some(alert.clone());
            },

            LocationZoneEvent::AlertDeactivated => {
                result.alert = None;
            },
        }

        Ok(ProcessResult::Changed(result))
    }
}