mod actor;
mod queries;
mod state;

pub use actor::{support::LocationZoneAggregateSupport, LocationZone, LocationZoneAggregate};
pub use errors::LocationZoneError;
pub use protocol::{LocationZoneCommand, LocationZoneEvent};
pub use queries::{
    WeatherProjection, WeatherView, ZONE_OFFSET_TABLE, ZONE_WEATHER_TABLE, ZONE_WEATHER_VIEW,
};
pub use services::{LocationServices, LocationServicesRef};

use crate::model::{LocationZoneCode, WeatherAlert};
use coerce::actor::system::ActorSystem;
use coerce::actor::IntoActorId;

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
    use super::errors::LocationZoneFailure;
    use crate::model::{LocationZoneCode, WeatherAlert, WeatherFrame, ZoneForecast};
    use coerce_cqrs::CommandResult;
    use strum_macros::Display;

    #[derive(Debug, Clone, PartialEq, JsonMessage, Serialize, Deserialize)]
    #[result("CommandResult<(), LocationZoneFailure>")]
    pub enum LocationZoneCommand {
        Subscribe(LocationZoneCode),
        Observe,
        Forecast,
        NoteObservation(Box<WeatherFrame>),
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
    use strum_macros::{Display, EnumDiscriminants};
    use thiserror::Error;

    #[derive(Debug, Error, EnumDiscriminants)]
    #[strum_discriminants(derive(Display, Serialize, Deserialize))]
    #[strum_discriminants(name(LocationZoneFailure))]
    pub enum LocationZoneError {
        #[error("{0}")]
        Noaa(#[from] crate::services::noaa::NoaaWeatherError),

        #[error("failed to persist: {0}")]
        Persist(#[from] coerce::persistent::PersistErr),

        #[error("failed to notify actor: {0}")]
        ActorRef(#[from] coerce::actor::ActorRefErr),

        #[error("failure in postgres storage: {0}")]
        PostgresStorage(#[from] coerce_cqrs::postgres::PostgresStorageError),

        #[error("{0}")]
        Projection(#[from] coerce_cqrs::projection::ProjectionError),
    }

    impl From<coerce::persistent::PersistErr> for LocationZoneFailure {
        fn from(error: coerce::persistent::PersistErr) -> Self {
            let zone_error: LocationZoneError = error.into();
            zone_error.into()
        }
    }

    // #[derive(Debug, Display, PartialEq, Eq, Serialize, Deserialize)]
    // pub enum LocationZoneFailure {
    //     Noaa,
    //     Server,
    // }

    // impl From<LocationZoneError> for LocationZoneFailure {
    //     fn from(error: LocationZoneError) -> Self {
    //         match error {
    //             LocationZoneError::Noaa(_) => Self::Noaa,
    //             LocationZoneError::ActorRef(_) => Self::Server,
    //         }
    //     }
    // }

    // impl From<coerce::persistent::PersistErr> for LocationZoneFailure {
    //     fn from(_error: coerce::persistent::PersistErr) -> Self {
    //         LocationZoneFailure::Server
    //     }
    // }
}