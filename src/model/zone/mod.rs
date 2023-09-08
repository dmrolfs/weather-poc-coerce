mod actor;
mod queries;
mod state;

use crate::model::{LocationZoneCode, WeatherAlert};
pub use actor::{LocationZone, LocationZoneAggregate};
use coerce::actor::system::ActorSystem;
use coerce::actor::IntoActorId;
pub use errors::LocationZoneError;
pub use protocol::{LocationZoneCommand, LocationZoneEvent};
pub use queries::WeatherView;
pub use services::{LocationServices, LocationServicesRef};

#[instrument(level = "trace", skip(system))]
pub async fn notify_observe(
    zone: &LocationZoneCode, system: &ActorSystem,
) -> Result<(), LocationZoneError> {
    if let Some(zone_ref) = system.get_tracked_actor::<LocationZone>(zone.into_actor_id()).await {
        zone_ref.notify(LocationZoneCommand::Observe)?;
    }
    Ok(())
}

#[instrument(level = "trace", skip(system))]
pub async fn notify_forecast(
    zone: &LocationZoneCode, system: &ActorSystem,
) -> Result<(), LocationZoneError> {
    if let Some(zone_ref) = system.get_tracked_actor::<LocationZone>(zone.into_actor_id()).await {
        zone_ref.notify(LocationZoneCommand::Forecast)?;
    }
    Ok(())
}

#[instrument(level = "trace", skip(system))]
pub async fn notify_update_alert(
    zone: &LocationZoneCode, alert: Option<WeatherAlert>, system: &ActorSystem,
) -> Result<(), LocationZoneError> {
    if let Some(zone_ref) = system.get_tracked_actor::<LocationZone>(zone.into_actor_id()).await {
        zone_ref.notify(LocationZoneCommand::NoteAlert(alert))?;
    }
    Ok(())
}

mod protocol {
    use crate::model::{LocationZoneCode, WeatherAlert, WeatherFrame, ZoneForecast};
    use coerce_cqrs::CommandResult;
    use strum_macros::Display;

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
    use crate::model::{LocationZoneCode, LocationZoneType, WeatherFrame, ZoneForecast};
    use crate::services::noaa::{NoaaWeatherError, NoaaWeatherServices, ZoneWeatherApi};
    use std::sync::Arc;

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
        async fn zone_observation(
            &self, zone: &LocationZoneCode,
        ) -> Result<WeatherFrame, NoaaWeatherError> {
            self.0.zone_observation(zone).await
        }

        async fn zone_forecast(
            &self, zone_type: LocationZoneType, zone: &LocationZoneCode,
        ) -> Result<ZoneForecast, NoaaWeatherError> {
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

        #[error("failed to notify actor: {0}")]
        ActorRef(#[from] coerce::actor::ActorRefErr),
    }
}
