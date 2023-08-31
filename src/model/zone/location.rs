use coerce::actor::context::ActorContext;
use coerce::actor::LocalActorRef;
use coerce::actor::message::{Handler, Message};
use coerce::persistent::{PersistentActor, PersistErr, Recover, RecoverSnapshot};
use coerce::persistent::types::JournalTypes;
use coerce_cqrs::{AggregateState, ApplyAggregateEvent, CommandResult, SnapshotTrigger};
use tagid::{Entity, Label};
use tracing::Instrument;
use crate::model::{LocationZoneCode, LocationZoneType, WeatherFrame, ZoneForecast};
use crate::model::zone::{LocationServicesRef, LocationZoneCommand, LocationZoneError, LocationZoneEvent};
use crate::services::noaa::ZoneWeatherApi;

pub type LocationZoneAggregate = LocalActorRef<LocationZone>;

// static SERVICES: OnceCell<LocationServicesRef> = OnceCell::new();

#[derive(Debug, Clone, Label,)]
pub struct LocationZone {
    state: LocationZoneState,
    services: LocationServicesRef,
    snapshot_trigger: SnapshotTrigger,
}

impl PartialEq for LocationZone {
    fn eq(&self, other: &Self) -> bool {
        self.state == other.state
    }
}

impl LocationZone {
    /// Initializes the `LocationServices` used by LocationZone actors. This may be initialized
    /// once, and will return the supplied value in an Err (i.e., `Err(services)`) on subsequent
    /// calls.
    // pub fn initialize_services(services: LocationServicesRef) -> Result<(), LocationServicesRef> {
    //     SERVICES.set(services)
    // }

    // fn services() -> LocationServicesRef {
    //     SERVICES.get().expect("LocationServices are not initialized").clone()
    // }

    fn location_zone_from_ctx(ctx: &ActorContext) -> LocationZoneCode {
        LocationZoneCode::new(ctx.id().to_string())
    }

    pub const fn new(services: LocationServicesRef) -> Self {
        Self {
            state: LocationZoneState::default(),
            services,
            snapshot_trigger: SnapshotTrigger::none(),
        }
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn with_snapshots(self, snapshot_after: u64) -> Self {
        Self {
            snapshot_trigger: SnapshotTrigger::on_event_count(snapshot_after),
            ..self
        }
    }

    async fn do_handle_snapshot(&mut self, ctx: &mut ActorContext) -> Result<(), PersistErr> {
        if self.snapshot_trigger.incr() {
            let snapshot_guard = info_span!("SNAPSHOT", id=%ctx.id(), trigger=?self.snapshot_trigger);
            let _g = snapshot_guard.enter();

            let snapshot = LocationZoneSnapshot {
                state: self.state.clone(),
                snapshot_trigger: self.snapshot_trigger.clone(),
            };
            self.snapshot(snapshot, ctx).await?;
        }
        Ok(())
    }

    #[instrument(level="debug", skip(self, ctx))]
    fn then_run(&self, command: LocationZoneCommand, ctx: &ActorContext) {
        match command {
            LocationZoneCommand::Observe => {
                self.do_observe(&Self::location_zone_from_ctx(ctx), ctx);
            },

            LocationZoneCommand::Forecast => {
                self.do_forecast(&Self::location_zone_from_ctx(ctx), ctx);
            },

            _ => {},
        }
    }

    fn do_observe(&self, zone: &LocationZoneCode, ctx: &ActorContext) {
        tokio::spawn(async {
            match self.services.zone_observation(zone).await {
                Ok(frame) => {
                    let self_ref  = ctx.actor_ref::<LocationZone>();
                    let note_cmd = LocationZoneCommand::NoteObservation(frame);
                    if let Err(error) = self_ref.notify(note_cmd) {
                        error!(?error, "failed to note observation of location zone: {zone} - ignoring");
                    }
                },
                Err(error) => {
                    error!(?error, "NOAA API call failed to observe location code {zone} - ignoring.");
                },
            }
        }
            .instrument(debug_span!("observe location zone", %zone))
        );
    }

    fn do_forecast(&self, zone: &LocationZoneCode, ctx: &ActorContext) {
        tokio::spawn(async {
            match self.services.zone_forecast(LocationZoneType::Forecast, zone).await {
                Ok(forecast) => {
                    let self_ref = ctx.actor_ref::<LocationZone>();
                    let note_cmd = LocationZoneCommand::NoteForecast(forecast);
                    if let Err(error) = self_ref.notify(note_cmd) {
                        error!(?error, "failed to note forecast of location zone: {zone} - ignoring");
                    }
                },
                Err(error) => {
                    error!(?error, "failed to get NOAA forecast for location code: {zone} - ignoring.");
                }
            }
        }
            .instrument(debug_span!("forecast location zone", %zone))
        );
    }
}

impl Entity for LocationZone {
    type IdGen = inner::LocationIdGenerator;
}

#[async_trait]
impl PersistentActor for LocationZone {
    #[instrument(level="info", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal
            .snapshot::<LocationZoneSnapshot>("location-zone-snapshot")
            .message::<LocationZoneEvent>("location-zone-event");
    }
}

#[async_trait]
impl Handler<LocationZoneCommand> for LocationZone {
    #[instrument(level="info", skip(ctx))]
    async fn handle(&mut self, command: LocationZoneCommand, ctx: &mut ActorContext) -> <LocationZoneCommand as Message>::Result {
        let events = match self.state.handle_command(command.clone(), ctx) {
            Ok(events) => events,
            Err(error) => return error.into(),
        };

        debug!("[{}] RESULTING EVENTS: {events:?}", ctx.id());
        for event in events {
            debug!("[{}] PERSISTING event: {event:?}", ctx.id());
            if let Err(error) = self.persist(&event, ctx).await {
                error!(?event, "[{}] failed to persist event: {error:?}", ctx.id());
                return error.into();
            }

            debug!("[{}] APPLYING event: {event:?}", ctx.id());
            if let Some(new_state) = self.state.apply_event(event, ctx) {
                self.state = new_state;
            }

            if let Err(error) = self.do_handle_snapshot(ctx).await {
                error!(?error, "failed to snapshot");
                return error.into();
            }
        }

        self.then_run(command, ctx);

        CommandResult::ok(())
    }
}

impl ApplyAggregateEvent<LocationZoneEvent> for LocationZone {
    type BaseType = Self;

    fn apply_event(&mut self, event: LocationZoneEvent, ctx: &mut ActorContext) -> Option<Self::BaseType> {
        if let Some(new_state) = self.state.apply_event(event, ctx) {
            self.state = new_state;
        }
        None
    }
}

#[async_trait]
impl Recover<LocationZoneEvent> for LocationZone {
    #[instrument(level="debug", skip(ctx))]
    async fn recover(&mut self, event: LocationZoneEvent, ctx: &mut ActorContext) {
        info!("[{}] RECOVERING from EVENT: {event:?}", ctx.id());
        if let Some(new_type) = self.apply_event(event, ctx) {
            *self = new_type;
        }
    }
}

#[derive(Debug, PartialEq, JsonSnapshot, Serialize, Deserialize)]
pub struct LocationZoneSnapshot {
    state: LocationZoneState,
    snapshot_trigger: SnapshotTrigger,
}

#[async_trait]
impl RecoverSnapshot<LocationZoneSnapshot> for LocationZone {
    #[instrument(level="debug", skip(ctx))]
    async fn recover(&mut self, snapshot: LocationZoneSnapshot, ctx: &mut ActorContext) {
        info!("[{}] RECOVERING from SNAPSHOT: {snapshot:?}", ctx.id());
        self.state = snapshot.state;
        self.snapshot_trigger = snapshot.snapshot_trigger;
    }

}

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

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuiescentLocationZone;

impl AggregateState<LocationZoneCommand, LocationZoneEvent> for QuiescentLocationZone {
    type Error = LocationZoneError;
    type State = LocationZoneState;

    #[instrument(level="debug", skip(_ctx))]
    fn handle_command(&self, command: LocationZoneCommand, _ctx: &mut ActorContext) -> Result<Vec<LocationZoneEvent>, Self::Error> {
        match command {
            LocationZoneCommand::Subscribe(zone) => {
                Ok(vec![LocationZoneEvent::Subscribed(zone)])
            },

            cmd => Err(LocationZoneError::RejectedCommand(format!(
                "LocationZone cannt handle command until it subscribes to a zone: {cmd:?}"
            )))
        }
    }

    #[instrument(level="debug", skip(_ctx))]
    fn apply_event(&mut self, event: LocationZoneEvent, _ctx: &mut ActorContext) -> Option<Self::State> {
        match event {
            LocationZoneEvent::Subscribed(zone_id) => {
                Some(LocationZoneState::Active(Box::new(ActiveLocationZone { zone_id, weather: None, forecast: None, active_alert: false, })))
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

    fn handle_command(&self, command: LocationZoneCommand, ctx: &mut ActorContext) -> Result<Vec<LocationZoneEvent>, Self::Error> {
        match command {
            LocationZoneCommand::Observe => Ok(vec!()),
            LocationZoneCommand::Forecast => Ok(vec!()),
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

    fn apply_event(&mut self, event: LocationZoneEvent, ctx: &mut ActorContext) -> Option<Self::State> {
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

        new_state
            .map(Box::new)
            .map(LocationZoneState::Active)
    }

}

#[async_trait]
impl AggregateState<LocationZoneCommand, LocationZoneEvent> for LocationZoneState {
    type Error = LocationZoneError;
    type State = Self;

    fn handle_command(&self, command: LocationZoneCommand, ctx: &mut ActorContext) -> Result<Vec<LocationZoneEvent>, Self::Error> {
        match self {
            Self::Quiescent(state) => state.handle_command(command, ctx),
            Self::Active(state) => state.handle_command(command, ctx),
        }
    }

    fn apply_event(&mut self, event: LocationZoneEvent, ctx: &mut ActorContext) -> Option<Self::State> {
        match self {
            Self::Quiescent(state) => state.apply_event(event, ctx),
            Self::Active(state) => state.apply_event(event, ctx),
        }
    }
}


mod inner {
    use tagid::IdGenerator;

    pub struct LocationIdGenerator;

    impl IdGenerator for LocationIdGenerator {
        type IdType = String;

        fn next_id_rep() -> Self::IdType {
            String::default()
        }
    }
}

