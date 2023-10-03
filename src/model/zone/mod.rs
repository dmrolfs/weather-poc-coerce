mod actor;
mod queries;
mod state;

pub use actor::{
    actor_id_from_zone, location_zone_for, support::LocationZoneAggregateSupport,
    zone_id_from_actor_id, LocationZone, LocationZoneAggregate, LocationZoneId,
};
pub use errors::LocationZoneError;
pub use protocol::{LocationZoneCommand, LocationZoneEvent};
pub use queries::{
    WeatherProjection, WeatherView, ZONE_OFFSET_TABLE, ZONE_WEATHER_TABLE, ZONE_WEATHER_VIEW,
};
pub use services::{initialize_services, LocationServices, LocationServicesRef};

use crate::model::{LocationZoneCode, WeatherAlert};
use coerce::actor::system::ActorSystem;
use coerce::actor::IntoActorId;

#[instrument(level = "debug", skip(system))]
pub async fn notify_observe(
    zone: &LocationZoneCode, system: &ActorSystem,
) -> Result<(), LocationZoneError> {
    let zone_actor_id = LocationZoneId::from(zone.clone()).into_actor_id();
    let zone_ref = location_zone_for(zone, system).await?;
    zone_ref.notify(LocationZoneCommand::Observe)?;
    Ok(())
}

#[instrument(level = "debug", skip(system))]
pub async fn notify_forecast(
    zone: &LocationZoneCode, system: &ActorSystem,
) -> Result<(), LocationZoneError> {
    let zone_actor_id = LocationZoneId::from(zone.clone()).into_actor_id();
    let zone_ref = location_zone_for(zone, system).await?;
    zone_ref.notify(LocationZoneCommand::Forecast)?;
    Ok(())
}

#[instrument(level = "debug", skip(system))]
pub async fn notify_update_alert(
    zone: &LocationZoneCode, alert: Option<WeatherAlert>, system: &ActorSystem,
) -> Result<(), LocationZoneError> {
    let zone_actor_id = LocationZoneId::from(zone.clone()).into_actor_id();
    let zone_ref = location_zone_for(zone, system).await?;
    zone_ref.notify(LocationZoneCommand::NoteAlert(alert))?;
    Ok(())
}

mod protocol {
    use super::errors::LocationZoneFailure;
    use crate::model::{WeatherAlert, WeatherFrame, ZoneForecast};
    use coerce_cqrs::CommandResult;
    use strum_macros::Display;

    #[derive(Debug, Clone, PartialEq, JsonMessage, Serialize, Deserialize)]
    #[result("CommandResult<(), LocationZoneFailure>")]
    pub enum LocationZoneCommand {
        Start,
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
        Started,
        ObservationAdded(Box<WeatherFrame>),
        ForecastUpdated(ZoneForecast),
        AlertActivated(WeatherAlert),
        AlertDeactivated,
    }
}

mod services {
    use crate::model::{LocationZoneCode, LocationZoneType, WeatherFrame, ZoneForecast};
    use crate::services::noaa::{NoaaWeatherError, NoaaWeatherServices, ZoneWeatherApi};
    use once_cell::sync::OnceCell;
    use std::sync::Arc;

    pub type LocationServicesRef = Arc<LocationServices>;

    static SERVICES: OnceCell<LocationServicesRef> = OnceCell::new();

    /// Initializes the `LocationServices` used by LocationZone actors. This may be initialized
    /// once, and will return the supplied value in an Err (i.e., `Err(services)`) on subsequent calls.
    pub fn initialize_services(services: LocationServicesRef) -> Result<(), LocationServicesRef> {
        SERVICES.set(services)
    }

    pub fn services() -> LocationServicesRef {
        SERVICES.get().expect("LocationServices are not initialized").clone()
    }

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
    use coerce::actor::ActorId;
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

        #[error("ActorId cannot be used as LocationZoneId: {0}")]
        BadActorId(ActorId),
    }

    impl From<coerce::persistent::PersistErr> for LocationZoneFailure {
        fn from(error: coerce::persistent::PersistErr) -> Self {
            let zone_error: LocationZoneError = error.into();
            zone_error.into()
        }
    }
}
