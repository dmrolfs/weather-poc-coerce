mod location_status;
mod saga;
mod controller;
mod services;
mod state;

use coerce::actor::system::ActorSystem;
use coerce::actor::ActorId;
pub use errors::UpdateLocationsError;
pub use protocol::{UpdateLocationsCommand, UpdateLocationsEvent};
pub use saga::{UpdateLocations, UpdateLocationsId, UpdateLocationsSaga};
pub use services::{UpdateLocationServices, UpdateLocationServicesRef};

use crate::connect::event_broadcast::EventBroadcastTopic;
use crate::connect::EventEnvelope;
use crate::model::zone::{LocationZoneError, LocationZoneEvent};
use crate::model::LocationZoneCode;
use tagid::Entity;

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

#[derive(Debug, Clone, PartialEq)]
pub struct LocationZoneBroadcastTopic;

impl EventBroadcastTopic for LocationZoneBroadcastTopic {
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
    use super::state::LocationUpdateStatus;
    use crate::connect::EventEnvelope;
    use crate::model::zone::LocationZoneEvent;
    use crate::model::LocationZoneCode;
    use coerce_cqrs::CommandResult;

    pub fn command_from_location_event(
        envelope: EventEnvelope<LocationZoneEvent>,
    ) -> Vec<UpdateLocationsCommand> {
        use LocationZoneEvent as E;
        use UpdateLocationsCommand as C;

        let zone = LocationZoneCode::new(envelope.source_id().as_ref());
        match envelope.event() {
            E::ObservationAdded(_) => vec![C::NoteLocationObservationUpdate(zone)],
            E::ForecastUpdated(_) => vec![C::NoteLocationForecastUpdate(zone)],
            E::AlertDeactivated | E::AlertActivated(_) => {
                vec![C::NoteLocationAlertStatusUpdate(zone)]
            },
            _ => vec![],
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, JsonMessage, Serialize, Deserialize)]
    #[result("CommandResult<()>")]
    pub enum UpdateLocationsCommand {
        UpdateLocations(Vec<LocationZoneCode>),
        NoteLocationObservationUpdate(LocationZoneCode),
        NoteLocationForecastUpdate(LocationZoneCode),
        NoteLocationAlertStatusUpdate(LocationZoneCode),
        NoteLocationsUpdateFailure(LocationZoneCode),
    }

    #[derive(Debug, Clone, PartialEq, JsonMessage, Serialize, Deserialize)]
    #[result("()")]
    pub enum UpdateLocationsEvent {
        Started(Vec<LocationZoneCode>),
        LocationUpdated(LocationZoneCode, LocationUpdateStatus),
        Completed,
        Failed,
    }
}

mod errors {
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum UpdateLocationsError {
        #[error("rejected command: {0}")]
        RejectedCommand(String),

        #[error("{0}")]
        Weather(#[from] crate::errors::WeatherError),

        #[error("failed to notify actor: {0}")]
        ActorRef(#[from] coerce::actor::ActorRefErr),
    }
}
