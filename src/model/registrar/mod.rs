mod actor;
mod services;

pub use actor::{Registrar, RegistrarAggregate};

mod protocol {
    use crate::model::LocationZoneCode;
    use coerce_cqrs::CommandResult;
    use strum_macros::Display;

    #[derive(Debug, Clone, JsonMessage, PartialEq, Eq, Serialize, Deserialize)]
    #[result("CommandResult<()>")]
    pub enum RegistrarCommand {
        UpdateWeather,
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

    // #[derive(Debug, Clone, JsonSnapshot, PartialEq, Eq, Serialize, Deserialize)]
    // pub struct RegistrarSnapshot {
    //     pub location_codes: HashSet<LocationZoneCode>,
    // }
}

mod errors {
    use crate::model::update::UpdateLocationsError;
    use crate::model::zone::LocationZoneError;
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum RegistrarError {
        #[error("{0}")]
        LocationZone(#[from] LocationZoneError),

        #[error("{0}")]
        UpdateForecastZones(#[from] UpdateLocationsError),

        #[error("rejected registrar command: {0}")]
        RejectedCommand(String),

        #[error("{0}")]
        ActorRef(#[from] coerce::actor::ActorRefErr),
    }
}
