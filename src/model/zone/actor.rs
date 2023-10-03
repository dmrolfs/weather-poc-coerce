use super::state::LocationZoneState;
use super::{LocationServicesRef, LocationZoneCommand, LocationZoneEvent};
use crate::model::zone::{
    services::services, LocationServices, LocationZoneAggregateSupport, LocationZoneError,
    WeatherView, ZONE_OFFSET_TABLE, ZONE_WEATHER_TABLE, ZONE_WEATHER_VIEW,
};
use crate::model::{LocationZoneCode, LocationZoneType};
use crate::services::noaa::{NoaaWeatherServices, ZoneWeatherApi};
use crate::{settings, Settings};
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::system::ActorSystem;
use coerce::actor::{ActorId, IntoActor, IntoActorId, LocalActorRef, ToActorId};
use coerce::persistent::types::JournalTypes;
use coerce::persistent::{PersistErr, PersistentActor, Recover, RecoverSnapshot};
use coerce_cqrs::postgres::PostgresProjectionStorage;
use coerce_cqrs::projection::processor::ProcessorSourceRef;
use coerce_cqrs::{Aggregate, AggregateState, CommandResult, SnapshotTrigger};
use std::sync::Arc;
use std::time::Duration;
use tagid::{Entity, Id, Label, Labeling};
use tracing::Instrument;

pub type LocationZoneId = Id<LocationZone, LocationZoneCode>;

impl From<LocationZoneCode> for LocationZoneId {
    fn from(zone: LocationZoneCode) -> Self {
        Self::for_labeled(zone)
    }
}

impl From<LocationZoneId> for LocationZoneCode {
    fn from(id: LocationZoneId) -> Self {
        id.id
    }
}

pub fn zone_id_from_actor_id(actor_id: ActorId) -> Result<LocationZoneId, LocationZoneError> {
    let parts: Vec<_> = actor_id.split(tagid::DELIMITER).collect();
    if parts.len() != 2 {
        return Err(LocationZoneError::BadActorId(actor_id));
    }

    let aggregate_name = parts[0];
    let aggregate_id = parts[1];

    if aggregate_name != LocationZone::labeler().label() {
        return Err(LocationZoneError::BadActorId(actor_id));
    }

    Ok(LocationZoneId::for_labeled(LocationZoneCode::new(
        aggregate_id,
    )))
}

pub fn actor_id_from_zone(zone: LocationZoneCode) -> ActorId {
    let aggregate_id: LocationZoneId = zone.into();
    aggregate_id.into_actor_id()
}

pub type LocationZoneAggregate = LocalActorRef<LocationZone>;

#[instrument(level = "debug", skip(system))]
pub async fn location_zone_for(
    zone: &LocationZoneCode, system: &ActorSystem,
) -> Result<LocationZoneAggregate, LocationZoneError> {
    let aggregate_id: LocationZoneId = zone.clone().into();
    let actor_id = aggregate_id.to_actor_id();
    match system.get_tracked_actor(actor_id.clone()).await {
        Some(actor_ref) => {
            debug!("DMR: Found LocationZone[{zone}]: {actor_ref:?}");
            Ok(actor_ref)
        },

        None => {
            let create = debug_span!("LocationZone Scheduling", %aggregate_id, %actor_id);
            let _create_guard = create.enter();

            let zone_aggregate = LocationZone::new(services())
                .with_snapshots(5)
                .into_actor(Some(aggregate_id), system)
                .await?;
            Ok(zone_aggregate)
        },
    }
}

#[derive(Debug, Clone, Label)]
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

//todo: make this into an `AggregateSupport` trait to be written via derive macro
// unique aggregate support + fn initialize_aggregate(...) ..
impl LocationZone {
    #[instrument(level = "debug", skip(journal_storage, settings, system))]
    pub async fn initialize_aggregate_support(
        journal_storage: ProcessorSourceRef, noaa: NoaaWeatherServices, settings: &Settings,
        system: &ActorSystem,
    ) -> Result<LocationZoneAggregateSupport, LocationZoneError> {
        let location_services = Arc::new(LocationServices::new(noaa));
        if let Err(svc) = crate::model::zone::initialize_services(location_services.clone()) {
            warn!(extra_service=?svc, "attempt to reinitialize LocationService - ignored");
        }

        let storage_config = settings::storage_config_from(&settings.database, &settings.zone);
        let weather_view_storage = PostgresProjectionStorage::<WeatherView>::new(
            ZONE_WEATHER_VIEW,
            Some(ZONE_WEATHER_TABLE.clone()),
            ZONE_OFFSET_TABLE.clone(),
            &storage_config,
            system,
        )
        .await?;
        let weather_projection = Arc::new(weather_view_storage);

        let weather_processor = support::weather_processor(
            journal_storage,
            weather_projection.clone(),
            Duration::from_secs(60),
            system,
        )?;

        Ok(LocationZoneAggregateSupport { weather_processor, weather_projection })
    }
}

impl LocationZone {
    pub fn new(services: LocationServicesRef) -> Self {
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
}

impl LocationZone {
    fn location_zone_from_ctx(ctx: &ActorContext) -> Result<LocationZoneCode, LocationZoneError> {
        zone_id_from_actor_id(ctx.id().clone()).map(|aggregate_id| aggregate_id.into())
    }

    async fn do_handle_snapshot(&mut self, ctx: &mut ActorContext) -> Result<(), PersistErr> {
        if self.snapshot_trigger.incr() {
            let snapshot_guard =
                info_span!("SNAPSHOT", id=%ctx.id(), trigger=?self.snapshot_trigger);
            let _g = snapshot_guard.enter();

            let snapshot = LocationZoneSnapshot {
                state: self.state.clone(),
                snapshot_trigger: self.snapshot_trigger,
            };
            self.snapshot(snapshot, ctx).await?;
        }
        Ok(())
    }

    #[instrument(level = "debug", skip(self, ctx))]
    fn then_run(&self, command: LocationZoneCommand, ctx: &ActorContext) {
        match command {
            LocationZoneCommand::Observe => match Self::location_zone_from_ctx(ctx) {
                Ok(zone_code) => self.do_observe(zone_code, ctx),
                Err(error) => warn!("cannot observe {}: {error:?} - skipping", ctx.id()),
            },

            LocationZoneCommand::Forecast => match Self::location_zone_from_ctx(ctx) {
                Ok(zone_code) => self.do_forecast(zone_code, ctx),
                Err(error) => warn!("cannot forecast {}: {error:?} - skipping", ctx.id()),
            },

            _ => {},
        }
    }

    #[instrument(level = "debug", skip(self, ctx))]
    fn do_observe(&self, zone: LocationZoneCode, ctx: &ActorContext) {
        let zone_0 = zone.clone();
        let services = self.services.clone();
        let self_ref = ctx.actor_ref::<Self>();

        tokio::spawn(
            async move {
                match services.zone_observation(&zone).await {
                    Ok(frame) => {
                        let note_cmd = LocationZoneCommand::NoteObservation(Box::new(frame));
                        if let Err(error) = self_ref.notify(note_cmd) {
                            error!(
                                ?error,
                                "failed to note observation of location zone: {zone} - ignoring"
                            );
                        }
                    },
                    Err(error) => {
                        error!(
                            ?error,
                            "NOAA API call failed to observe location code {zone} - ignoring."
                        );
                    },
                }
            }
            .instrument(debug_span!("observe location zone", zone=%zone_0)),
        );
    }

    #[instrument(level = "debug", skip(self, ctx))]
    fn do_forecast(&self, zone: LocationZoneCode, ctx: &ActorContext) {
        let zone_0 = zone.clone();
        let services = self.services.clone();
        let self_ref = ctx.actor_ref::<Self>();

        tokio::spawn(
            async move {
                match services.zone_forecast(LocationZoneType::Forecast, &zone).await {
                    Ok(forecast) => {
                        let note_cmd = LocationZoneCommand::NoteForecast(forecast);
                        if let Err(error) = self_ref.notify(note_cmd) {
                            error!(
                                ?error,
                                "failed to note forecast of location zone: {zone} - ignoring"
                            );
                        }
                    },
                    Err(error) => {
                        error!(
                            ?error,
                            "failed to get NOAA forecast for location code: {zone} - ignoring."
                        );
                    },
                }
            }
            .instrument(debug_span!("forecast location zone", zone=%zone_0)),
        );
    }
}

impl Entity for LocationZone {
    type IdGen = inner::LocationIdGenerator;
}

impl Aggregate for LocationZone {}

#[async_trait]
impl PersistentActor for LocationZone {
    #[instrument(level = "info", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal
            .snapshot::<LocationZoneSnapshot>(&Self::journal_snapshot_type_identifier::<
                LocationZoneSnapshot,
            >())
            .message::<LocationZoneEvent>(&Self::journal_message_type_identifier::<
                LocationZoneEvent,
            >());
    }
}

#[async_trait]
impl Handler<LocationZoneCommand> for LocationZone {
    #[instrument(level = "info", skip(self, ctx))]
    async fn handle(
        &mut self, command: LocationZoneCommand, ctx: &mut ActorContext,
    ) -> <LocationZoneCommand as Message>::Result {
        let events = match self.state.handle_command(&command) {
            CommandResult::Ok(events) => events,
            CommandResult::Rejected(msg) => return CommandResult::Rejected(msg),
            CommandResult::Err(error) => {
                error!(
                    ?command,
                    ?error,
                    "LocationZone({zone}) command failed",
                    zone = ctx.id()
                );
                return CommandResult::Err(error.into());
            },
        };

        debug!("[{}] RESULTING EVENTS: {events:?}", ctx.id());
        for event in events {
            debug!("[{}] PERSISTING event: {event:?}", ctx.id());
            if let Err(error) = self.persist(&event, ctx).await {
                error!(?event, "[{}] failed to persist event: {error:?}", ctx.id());
                return CommandResult::Err(error.into());
            }

            debug!("[{}] APPLYING event: {event:?}", ctx.id());
            if let Some(new_state) = self.state.apply_event(event) {
                self.state = new_state;
            }

            if let Err(error) = self.do_handle_snapshot(ctx).await {
                error!(?error, "failed to snapshot");
                return CommandResult::Err(error.into());
            }
        }

        self.then_run(command, ctx);

        CommandResult::ok(())
    }
}

#[async_trait]
impl Recover<LocationZoneEvent> for LocationZone {
    #[instrument(level = "debug", skip(ctx))]
    async fn recover(&mut self, event: LocationZoneEvent, ctx: &mut ActorContext) {
        info!("[{}] RECOVERING from EVENT: {event:?}", ctx.id());
        if let Some(new_type) = self.state.apply_event(event) {
            self.state = new_type;
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
    #[instrument(level = "debug", skip(ctx))]
    async fn recover(&mut self, snapshot: LocationZoneSnapshot, ctx: &mut ActorContext) {
        info!("[{}] RECOVERING from SNAPSHOT: {snapshot:?}", ctx.id());
        self.state = snapshot.state;
        self.snapshot_trigger = snapshot.snapshot_trigger;
    }
}

pub mod support {
    use crate::model::zone::{LocationZone, WeatherProjection, WeatherView, ZONE_WEATHER_VIEW};
    use coerce::actor::system::ActorSystem;
    use coerce_cqrs::projection::processor::{
        Processor, ProcessorEngineRef, ProcessorSourceRef, RegularInterval,
    };
    use coerce_cqrs::projection::{
        PersistenceId, ProjectionApplicator, ProjectionError, ProjectionStorageRef,
    };
    use once_cell::sync::OnceCell;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug, Clone)]
    pub struct LocationZoneAggregateSupport {
        pub weather_processor: ProcessorEngineRef,
        pub weather_projection: WeatherProjection,
    }

    static WEATHER_PROCESSOR: OnceCell<ProcessorEngineRef> = OnceCell::new();
    pub fn weather_processor(
        journal_storage: ProcessorSourceRef,
        view_storage: ProjectionStorageRef<PersistenceId, WeatherView>, interval: Duration,
        system: &ActorSystem,
    ) -> Result<ProcessorEngineRef, ProjectionError> {
        let processor = WEATHER_PROCESSOR.get_or_try_init(|| {
            let weather_apply =
                ProjectionApplicator::<LocationZone, _, _, _>::new(WeatherView::apply_event);

            Processor::builder_for::<LocationZone, _, _, _, _>(ZONE_WEATHER_VIEW)
                .with_entry_handler(weather_apply)
                .with_system(system.clone())
                .with_source(journal_storage.clone())
                .with_projection_storage(view_storage.clone())
                .with_interval_calculator(RegularInterval::of_duration(interval))
                .finish()
                .map_err(|err| err.into())
                .and_then(|engine| engine.run())
                .map(Arc::new)
        })?;

        Ok(processor.clone())
    }
}

mod inner {
    use super::LocationZoneId;
    use tagid::IdGenerator;

    pub struct LocationIdGenerator;

    impl IdGenerator for LocationIdGenerator {
        type IdType = LocationZoneId;

        fn next_id_rep() -> Self::IdType {
            unimplemented!("use location zone code to create aggregate id")
        }
    }
}
