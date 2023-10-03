mod actor;
mod services;
mod view;

pub use actor::{
    registrar_actor, singleton_id, support::RegistrarAggregateSupport, Registrar,
    RegistrarAggregate,
};
pub use errors::{RegistrarError, RegistrarFailure};
pub use services::{RegistrarServices, RegistrarServicesRef};
pub use view::{MonitoredZonesProjection, MonitoredZonesView, MONITORED_ZONES_VIEW};

use crate::model::registrar::protocol::{RegistrarAdminCommand as AC, UpdateWeather};
use crate::model::update::UpdateLocationsId;
use crate::model::LocationZoneCode;
use coerce::actor::system::ActorSystem;
use coerce_cqrs::CommandResult;

#[instrument(level = "debug", skip(system))]
pub async fn update_weather(
    system: &ActorSystem,
) -> Result<Option<UpdateLocationsId>, RegistrarFailure> {
    result_from(registrar_actor(system).await?.send(UpdateWeather).await?)
}

#[instrument(level = "debug", skip(system))]
pub async fn clear_monitoring(system: &ActorSystem) -> Result<(), RegistrarFailure> {
    result_from(registrar_actor(system).await?.send(AC::ClearZoneMonitoring).await?)
}

#[instrument(level = "debug", skip(system))]
pub async fn monitor_forecast_zone(
    zone: LocationZoneCode, system: &ActorSystem,
) -> Result<(), RegistrarFailure> {
    let registrar_ref = registrar_actor(system).await;
    debug!(?registrar_ref, "DMR: REGISTRAR_ACTOR");
    result_from(registrar_ref?.send(AC::MonitorForecastZone(zone)).await?)
}

#[instrument(level = "debug", skip(system))]
pub async fn forget_forecast_zone(
    zone: LocationZoneCode, system: &ActorSystem,
) -> Result<(), RegistrarFailure> {
    result_from(registrar_actor(system).await?.send(AC::ForgetForecastZone(zone)).await?)
}

fn result_from<T, E>(command_result: CommandResult<T, E>) -> Result<T, RegistrarFailure>
where
    E: std::fmt::Display + Into<RegistrarFailure>,
{
    match command_result {
        CommandResult::Ok(x) => Ok(x),
        CommandResult::Rejected(msg) => Err(RegistrarError::RejectedCommand(msg).into()),
        CommandResult::Err(error) => Err(error.into()),
    }
}

mod protocol {
    use super::errors::RegistrarFailure;
    use crate::model::update::UpdateLocationsId;
    use crate::model::LocationZoneCode;
    use coerce_cqrs::CommandResult;
    use strum_macros::Display;

    #[derive(Debug, Clone, JsonMessage, PartialEq, Eq, Serialize, Deserialize)]
    #[result("CommandResult<Option<UpdateLocationsId>, RegistrarFailure>")]
    pub struct UpdateWeather;

    #[derive(Debug, Clone, JsonMessage, PartialEq, Eq, Serialize, Deserialize)]
    #[result("CommandResult<(), RegistrarFailure>")]
    pub enum RegistrarAdminCommand {
        MonitorForecastZone(LocationZoneCode),
        ClearZoneMonitoring,
        ForgetForecastZone(LocationZoneCode),
    }

    #[derive(Debug, Display, Clone, PartialEq, Eq, JsonMessage, Serialize, Deserialize)]
    #[result("()")]
    pub enum RegistrarEvent {
        ForecastZoneAdded(LocationZoneCode),
        ForecastZoneForgotten(LocationZoneCode),
        AllForecastZonesForgotten,
    }
}

mod errors {
    use strum_macros::{Display, EnumDiscriminants};
    use thiserror::Error;

    #[derive(Debug, Error, EnumDiscriminants)]
    #[strum_discriminants(derive(Display, Serialize, Deserialize))]
    #[strum_discriminants(name(RegistrarFailure))]
    pub enum RegistrarError {
        #[error("{0}")]
        LocationZone(#[from] crate::model::zone::LocationZoneError),

        #[error("{0}")]
        UpdateLocations(#[from] crate::model::update::UpdateLocationsError),

        #[error("{0}")]
        ActorRef(#[from] coerce::actor::ActorRefErr),

        #[error("failed to persist: {0}")]
        Persist(#[from] coerce::persistent::PersistErr),

        #[error("failure in postgres storage: {0}")]
        PostgresStorage(#[from] coerce_cqrs::postgres::PostgresStorageError),

        #[error("projection failure: {0}")]
        Projection(#[from] coerce_cqrs::projection::ProjectionError),

        #[error("command rejected: {0}")]
        RejectedCommand(String),
    }

    impl From<coerce::actor::ActorRefErr> for RegistrarFailure {
        fn from(error: coerce::actor::ActorRefErr) -> Self {
            let reg_error: RegistrarError = error.into();
            reg_error.into()
        }
    }

    impl From<coerce::persistent::PersistErr> for RegistrarFailure {
        fn from(error: coerce::persistent::PersistErr) -> Self {
            let reg_error: RegistrarError = error.into();
            reg_error.into()
        }
    }
}
