mod queries;
mod location;

pub use errors::LocationZoneError;
pub use location::{LocationZone, LocationZoneAggregate};
pub use protocol::{LocationZoneCommand, LocationZoneEvent};
pub use queries::WeatherView;
pub use services::{LocationServices, LocationServicesRef};

mod protocol {
    use strum_macros::Display;
    use crate::model::{LocationZoneCode, WeatherAlert, WeatherFrame, ZoneForecast};
    use coerce_cqrs::CommandResult;

    #[derive(Debug, Clone, PartialEq, JsonMessage, Serialize, Deserialize)]
    #[result("CommandResult<()>")]
    pub enum LocationZoneCommand {
        Subscribe(LocationZoneCode),
        Observe,
        Forecast,
        NoteObservation(WeatherFrame),
        NoteForecast(ZoneForecast),
        NoteAlert(Option<WeatherAlert>),
    }

    #[derive(Debug, Display, Clone, JsonMessage, PartialEq, Serialize, Deserialize)]
    #[strum(serialize_all = "snake_case")]
    #[result("()")]
    pub enum LocationZoneEvent {
        Subscribed(LocationZoneCode),
        ObservationAdded(Box<WeatherFrame>),
        ForecastUpdated(ZoneForecast),
        AlertActivated(WeatherAlert),
        AlertDeactivated,
    }
}

mod services {
    use std::sync::Arc;
    use crate::model::{LocationZoneCode, LocationZoneType, WeatherFrame, ZoneForecast};
    use crate::services::noaa::{NoaaWeatherError, NoaaWeatherServices, ZoneWeatherApi};

    pub type LocationServicesRef = Arc<LocationServices>;

    #[derive(Debug, Clone)]
    pub struct LocationServices(NoaaWeatherServices);

    impl LocationServices {
        pub fn new(noaa: NoaaWeatherServices) -> Self {
            Self(noaa)
        }
    }

    #[async_trait]
    impl ZoneWeatherApi for LocationServices {
        async fn zone_observation(&self, zone: &LocationZoneCode) -> Result<WeatherFrame, NoaaWeatherError> {
            self.0.zone_observation(zone).await
        }

        async fn zone_forecast(&self, zone_type: LocationZoneType, zone: &LocationZoneCode) -> Result<ZoneForecast, NoaaWeatherError> {
            self.0.zone_forecast(zone_type, zone).await
        }
    }
}

mod errors {
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum LocationZoneError {
        #[error("rejected command: {0}")]
        RejectedCommand(String),

        #[error("{0}")]
        Noaa(#[from] crate::services::noaa::NoaaWeatherError),
    }
}
