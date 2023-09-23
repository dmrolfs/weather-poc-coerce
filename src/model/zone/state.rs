use super::{LocationZoneCommand, LocationZoneError, LocationZoneEvent};
use crate::model::{WeatherFrame, ZoneForecast};
use coerce_cqrs::{AggregateState, CommandResult};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LocationZoneState {
    Quiescent(QuiescentLocationZone),
    Active(Box<ActiveLocationZone>),
}

impl Default for LocationZoneState {
    fn default() -> Self {
        Self::Quiescent(QuiescentLocationZone)
    }
}

#[async_trait]
impl AggregateState<LocationZoneCommand, LocationZoneEvent> for LocationZoneState {
    type Error = LocationZoneError;
    type State = Self;

    fn handle_command(
        &self, command: &LocationZoneCommand,
    ) -> CommandResult<Vec<LocationZoneEvent>, Self::Error> {
        match self {
            Self::Quiescent(state) => state.handle_command(command),
            Self::Active(state) => state.handle_command(command),
        }
    }

    fn apply_event(&mut self, event: LocationZoneEvent) -> Option<Self::State> {
        match self {
            Self::Quiescent(state) => state.apply_event(event),
            Self::Active(state) => state.apply_event(event),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuiescentLocationZone;

impl AggregateState<LocationZoneCommand, LocationZoneEvent> for QuiescentLocationZone {
    type Error = LocationZoneError;
    type State = LocationZoneState;

    #[instrument(level = "debug")]
    fn handle_command(
        &self, command: &LocationZoneCommand,
    ) -> CommandResult<Vec<LocationZoneEvent>, Self::Error> {
        match command {
            LocationZoneCommand::Start => CommandResult::Ok(vec![LocationZoneEvent::Started]),

            cmd => CommandResult::Rejected(format!(
                "LocationZone cannt handle command until it subscribes to a zone: {cmd:?}"
            )),
        }
    }

    #[instrument(level = "debug")]
    fn apply_event(&mut self, event: LocationZoneEvent) -> Option<Self::State> {
        match event {
            LocationZoneEvent::Started => {
                Some(LocationZoneState::Active(Box::new(ActiveLocationZone {
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
    pub weather: Option<WeatherFrame>,
    pub forecast: Option<ZoneForecast>,
    pub active_alert: bool,
}

impl AggregateState<LocationZoneCommand, LocationZoneEvent> for ActiveLocationZone {
    type Error = LocationZoneError;
    type State = LocationZoneState;

    fn handle_command(
        &self, command: &LocationZoneCommand,
    ) -> CommandResult<Vec<LocationZoneEvent>, Self::Error> {
        match command {
            LocationZoneCommand::Observe => CommandResult::Ok(vec![]),
            LocationZoneCommand::Forecast => CommandResult::Ok(vec![]),
            LocationZoneCommand::NoteObservation(frame) => {
                CommandResult::Ok(vec![LocationZoneEvent::ObservationAdded(frame.clone())])
            },
            LocationZoneCommand::NoteForecast(forecast) => {
                CommandResult::Ok(vec![LocationZoneEvent::ForecastUpdated(forecast.clone())])
            },
            LocationZoneCommand::NoteAlert(alert) => {
                let event = match (self.active_alert, alert) {
                    (false, Some(alert)) => Some(LocationZoneEvent::AlertActivated(alert.clone())),
                    (true, None) => Some(LocationZoneEvent::AlertDeactivated),
                    _ => None,
                };

                CommandResult::Ok(event.into_iter().collect())
            },
            LocationZoneCommand::Start => {
                debug!("zone subscribe previously set - ignoring");
                CommandResult::Ok(vec![])
            },
        }
    }

    fn apply_event(&mut self, event: LocationZoneEvent) -> Option<Self::State> {
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
