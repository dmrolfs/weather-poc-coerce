use super::{LocationZoneCommand, LocationZoneError, LocationZoneEvent};
use crate::model::{LocationZoneCode, WeatherFrame, ZoneForecast};
use coerce::actor::context::ActorContext;
use coerce_cqrs::AggregateState;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LocationZoneState {
    Quiescent(QuiescentLocationZone),
    Active(Box<ActiveLocationZone>),
}

impl Default for LocationZoneState {
    fn default() -> Self {
        Self::Quiescent(QuiescentLocationZone::default())
    }
}

#[async_trait]
impl AggregateState<LocationZoneCommand, LocationZoneEvent> for LocationZoneState {
    type Error = LocationZoneError;
    type State = Self;

    fn handle_command(
        &self, command: LocationZoneCommand, ctx: &mut ActorContext,
    ) -> Result<Vec<LocationZoneEvent>, Self::Error> {
        match self {
            Self::Quiescent(state) => state.handle_command(command, ctx),
            Self::Active(state) => state.handle_command(command, ctx),
        }
    }

    fn apply_event(
        &mut self, event: LocationZoneEvent, ctx: &mut ActorContext,
    ) -> Option<Self::State> {
        match self {
            Self::Quiescent(state) => state.apply_event(event, ctx),
            Self::Active(state) => state.apply_event(event, ctx),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuiescentLocationZone;

impl AggregateState<LocationZoneCommand, LocationZoneEvent> for QuiescentLocationZone {
    type Error = LocationZoneError;
    type State = LocationZoneState;

    #[instrument(level = "debug", skip(_ctx))]
    fn handle_command(
        &self, command: LocationZoneCommand, _ctx: &mut ActorContext,
    ) -> Result<Vec<LocationZoneEvent>, Self::Error> {
        match command {
            LocationZoneCommand::Subscribe(zone) => Ok(vec![LocationZoneEvent::Subscribed(zone)]),

            cmd => Err(LocationZoneError::RejectedCommand(format!(
                "LocationZone cannt handle command until it subscribes to a zone: {cmd:?}"
            ))),
        }
    }

    #[instrument(level = "debug", skip(_ctx))]
    fn apply_event(
        &mut self, event: LocationZoneEvent, _ctx: &mut ActorContext,
    ) -> Option<Self::State> {
        match event {
            LocationZoneEvent::Subscribed(zone_id) => {
                Some(LocationZoneState::Active(Box::new(ActiveLocationZone {
                    zone_id,
                    weather: None,
                    forecast: None,
                    active_alert: false,
                })))
            },

            event => {
                warn!(?event, "invalid quiescent location zone event -- ignored");
                None
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActiveLocationZone {
    pub zone_id: LocationZoneCode,
    pub weather: Option<WeatherFrame>,
    pub forecast: Option<ZoneForecast>,
    pub active_alert: bool,
}

impl AggregateState<LocationZoneCommand, LocationZoneEvent> for ActiveLocationZone {
    type Error = LocationZoneError;
    type State = LocationZoneState;

    fn handle_command(
        &self, command: LocationZoneCommand, ctx: &mut ActorContext,
    ) -> Result<Vec<LocationZoneEvent>, Self::Error> {
        match command {
            LocationZoneCommand::Observe => Ok(vec![]),
            LocationZoneCommand::Forecast => Ok(vec![]),
            LocationZoneCommand::NoteObservation(frame) => {
                Ok(vec![LocationZoneEvent::ObservationAdded(Box::new(frame))])
            },
            LocationZoneCommand::NoteForecast(forecast) => {
                Ok(vec![LocationZoneEvent::ForecastUpdated(forecast)])
            },
            LocationZoneCommand::NoteAlert(alert) => {
                let event = match (self.active_alert, alert) {
                    (false, Some(alert)) => Some(LocationZoneEvent::AlertActivated(alert)),
                    (true, None) => Some(LocationZoneEvent::AlertDeactivated),
                    _ => None,
                };

                Ok(event.into_iter().collect())
            },
            LocationZoneCommand::Subscribe(new_zone) => {
                debug!("{new_zone} zone subscribe previously set - ignoring");
                Ok(vec![])
            },
        }
    }

    fn apply_event(
        &mut self, event: LocationZoneEvent, ctx: &mut ActorContext,
    ) -> Option<Self::State> {
        use LocationZoneEvent::*;

        let new_state = match event {
            ObservationAdded(frame) => Some(Self { weather: Some(*frame), ..self.clone() }),

            ForecastUpdated(forecast) => Some(Self { forecast: Some(forecast), ..self.clone() }),

            AlertActivated(_) => Some(Self { active_alert: true, ..self.clone() }),

            AlertDeactivated => Some(Self { active_alert: false, ..self.clone() }),

            event => {
                warn!(?event, "invalid active location zone event -- ignored");
                None
            },
        };

        new_state.map(Box::new).map(LocationZoneState::Active)
    }
}
