mod controller;
mod location_status;
mod saga;
mod services;
mod state;
mod view;

pub use errors::UpdateLocationsError;
pub use protocol::{UpdateLocationsCommand, UpdateLocationsEvent};
pub use saga::{
    support::UpdateLocationsAggregateSupport, update_locations_saga, UpdateLocations,
    UpdateLocationsId, UpdateLocationsSaga,
};
pub use services::{UpdateLocationServices, UpdateLocationServicesRef};
pub use state::{
    ActiveLocationsUpdate, FinishedLocationsUpdate, QuiescentLocationsUpdate, UpdateLocationsState,
};
pub use view::{UpdateLocationsHistory, UpdateLocationsHistoryProjection};

use crate::connect::{EventCommandTopic, EventEnvelope};
use crate::model::zone::{LocationZoneError, LocationZoneEvent};
use crate::model::{LocationZone, LocationZoneCode};
use coerce::actor::system::ActorSystem;
use coerce::actor::ActorId;
use coerce_cqrs::postgres::{PostgresStorageConfig, TableName};
use once_cell::sync::Lazy;
use tagid::{Entity, Label};

#[inline]
pub fn generate_id() -> UpdateLocationsId {
    UpdateLocations::next_id()
}

#[instrument(level = "trace", skip(system))]
async fn note_zone_alert_status_updated(
    saga_id: ActorId, zone: LocationZoneCode, system: &ActorSystem,
) -> Result<(), UpdateLocationsError> {
    if let Some(saga_ref) = system.get_tracked_actor::<UpdateLocations>(saga_id).await {
        saga_ref.notify(UpdateLocationsCommand::NoteLocationAlertStatusUpdate(zone))?;
    }
    Ok(())
}

#[instrument(level = "trace", skip(system))]
async fn note_zone_update_failure(
    saga_id: ActorId, zone: LocationZoneCode, error: LocationZoneError, system: &ActorSystem,
) -> Result<(), UpdateLocationsError> {
    if let Some(saga_ref) = system.get_tracked_actor::<UpdateLocations>(saga_id).await {
        saga_ref.notify(UpdateLocationsCommand::NoteLocationsUpdateFailure(zone))?;
    }
    Ok(())
}

#[instrument(level = "trace", skip(system))]
async fn note_alerts_updated(
    saga_id: ActorId, system: &ActorSystem,
) -> Result<(), UpdateLocationsError> {
    if let Some(saga_ref) = system.get_tracked_actor::<UpdateLocations>(saga_id).await {
        saga_ref.notify(UpdateLocationsCommand::NoteAlertsUpdated)?;
    }
    Ok(())
}

pub const UPDATE_LOCATION_ZONE_SUBSCRIPTION: &str = "update_location_zone_subscription";
pub static UPDATE_LOCATION_ZONE_SUBSCRIPTION_OFFSET_TABLE: Lazy<TableName> =
    Lazy::new(PostgresStorageConfig::default_projection_offsets_table); //"projection_offset";

#[derive(Debug, Clone, Label, PartialEq)]
pub struct LocationZoneBroadcastTopic;

impl EventCommandTopic for LocationZoneBroadcastTopic {
    type Source = LocationZone;
    type Subscriber = UpdateLocations;
    type Event = LocationZoneEvent;
    type Command = UpdateLocationsCommand;

    fn commands_from_event(
        &self, event_envelope: &EventEnvelope<Self::Event>,
    ) -> Vec<Self::Command> {
        let zone = LocationZoneCode::new(event_envelope.source_id().as_ref());
        match event_envelope.event() {
            Self::Event::ObservationAdded(_) => {
                vec![Self::Command::NoteLocationObservationUpdate(zone)]
            },
            Self::Event::ForecastUpdated(_) => {
                vec![Self::Command::NoteLocationForecastUpdate(zone)]
            },
            Self::Event::AlertDeactivated | Self::Event::AlertActivated(_) => {
                vec![Self::Command::NoteLocationAlertStatusUpdate(zone)]
            },
            _ => vec![],
        }
    }
}

mod protocol {
    use super::errors::UpdateLocationsFailure;
    use crate::model::update::state::LocationUpdateStatus;
    use crate::model::LocationZoneCode;
    use coerce_cqrs::CommandResult;
    use strum_macros::Display;

    #[derive(Debug, Clone, PartialEq, Eq, JsonMessage, Serialize, Deserialize)]
    #[result("CommandResult<(), UpdateLocationsFailure>")]
    pub enum UpdateLocationsCommand {
        UpdateLocations(Vec<LocationZoneCode>),
        NoteLocationObservationUpdate(LocationZoneCode),
        NoteLocationForecastUpdate(LocationZoneCode),
        NoteLocationAlertStatusUpdate(LocationZoneCode),
        NoteLocationsUpdateFailure(LocationZoneCode),
        NoteAlertsUpdated,
    }

    #[derive(Debug, Display, Clone, PartialEq, JsonMessage, ToSchema, Serialize, Deserialize)]
    #[result("()")]
    #[strum(serialize_all = "snake_case")]
    pub enum UpdateLocationsEvent {
        Started(Vec<LocationZoneCode>),
        LocationUpdated(LocationZoneCode, LocationUpdateStatus),
        AlertsUpdated,
        Completed,
        Failed,
    }
}

mod errors {
    use strum_macros::{Display, EnumDiscriminants};
    use thiserror::Error;

    #[derive(Debug, Error, EnumDiscriminants)]
    #[strum_discriminants(derive(Display, Serialize, Deserialize))]
    #[strum_discriminants(name(UpdateLocationsFailure))]
    pub enum UpdateLocationsError {
        #[error("{0}")]
        Noaa(#[from] crate::services::noaa::NoaaWeatherError),

        #[error("{0}")]
        Connect(#[from] crate::connect::ConnectError),

        #[error("failed to persist: {0}")]
        Persist(#[from] coerce::persistent::PersistErr),

        #[error("projection failure: {0}")]
        Projection(#[from] coerce_cqrs::projection::ProjectionError),

        #[error("failed to notify actor: {0}")]
        ActorRef(#[from] coerce::actor::ActorRefErr),

        #[error("{0}")]
        ParseUrl(#[from] url::ParseError),
    }

    impl From<coerce::persistent::PersistErr> for UpdateLocationsFailure {
        fn from(error: coerce::persistent::PersistErr) -> Self {
            let update_err: UpdateLocationsError = error.into();
            update_err.into()
        }
    }
}
