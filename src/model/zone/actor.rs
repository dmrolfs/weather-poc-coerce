use super::state::LocationZoneState;
use super::{LocationServicesRef, LocationZoneCommand, LocationZoneEvent};
use crate::model::zone::{
    LocationZoneAggregateSupport, LocationZoneError, WeatherView, ZONE_OFFSET_TABLE,
    ZONE_WEATHER_TABLE, ZONE_WEATHER_VIEW, services::services
};
use crate::model::{LocationZoneCode, LocationZoneType};
use crate::services::noaa::ZoneWeatherApi;
use crate::{settings, Settings};
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::system::ActorSystem;
use coerce::actor::LocalActorRef;
use coerce::persistent::types::JournalTypes;
use coerce::persistent::{PersistErr, PersistentActor, Recover, RecoverSnapshot};
use coerce_cqrs::postgres::PostgresProjectionStorage;
use coerce_cqrs::projection::processor::ProcessorSourceRef;
use coerce_cqrs::{AggregateState, CommandResult, SnapshotTrigger};
use std::sync::Arc;
use tagid::{Entity, Label};
use tracing::Instrument;

pub type LocationZoneId = LocationZoneCode;
pub type LocationZoneAggregate = LocalActorRef<LocationZone>;

pub async fn location_zone_for(zone: &LocationZoneCode, system: &ActorSystem) -> Result<LocationZoneAggregate, LocationZoneError> {
    use coerce::actor::IntoActor;

    let aggregate_id = zone.to_string();
    let aggregate = LocationZone::new(services()).into_actor(Some(aggregate_id), system).await?;
    Ok(aggregate)
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

// impl Default for LocationZone {
//     fn default() -> Self {
//         Self::new(Self::services())
//     }
// }

//todo: make this into an `AggregateSupport` trait to be written via derive macro
// unique aggregate support + fn initialize_aggregate(...) ..
impl LocationZone {
    pub async fn initialize_aggregate_support(
        journal_storage: ProcessorSourceRef, settings: &Settings, system: &ActorSystem,
    ) -> Result<LocationZoneAggregateSupport, LocationZoneError> {
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

        let weather_processor =
            support::weather_processor(journal_storage, weather_projection.clone(), system)?;

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
    fn location_zone_from_ctx(ctx: &ActorContext) -> LocationZoneCode {
        LocationZoneCode::new(ctx.id().to_string())
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
            LocationZoneCommand::Observe => {
                self.do_observe(Self::location_zone_from_ctx(ctx), ctx);
            },

            LocationZoneCommand::Forecast => {
                self.do_forecast(Self::location_zone_from_ctx(ctx), ctx);
            },

            _ => {},
        }
    }

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

#[async_trait]
impl PersistentActor for LocationZone {
    #[instrument(level = "info", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal
            .snapshot::<LocationZoneSnapshot>("location-zone-snapshot")
            .message::<LocationZoneEvent>("location-zone-event");
    }
}

#[async_trait]
impl Handler<LocationZoneCommand> for LocationZone {
    #[instrument(level = "info", skip(ctx))]
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

// impl ApplyAggregateEvent<LocationZoneEvent> for LocationZone {
//     type BaseType = Self;
//
//     fn apply_event(
//         &mut self, event: LocationZoneEvent, ctx: &mut ActorContext,
//     ) -> Option<Self::BaseType> {
//         if let Some(new_state) = self.state.apply_event(event, ctx) {
//             self.state = new_state;
//         }
//         None
//     }
// }

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
        view_storage: ProjectionStorageRef<PersistenceId, WeatherView>, system: &ActorSystem,
    ) -> Result<ProcessorEngineRef, ProjectionError> {
        let processor = WEATHER_PROCESSOR.get_or_try_init(|| {
            let weather_apply = ProjectionApplicator::new(WeatherView::apply_event);

            Processor::builder_for::<LocationZone, _, _, _, _>(ZONE_WEATHER_VIEW)
                .with_entry_handler(weather_apply)
                .with_system(system.clone())
                .with_source(journal_storage.clone())
                .with_projection_storage(view_storage.clone())
                .with_interval_calculator(RegularInterval::of_duration(Duration::from_millis(250)))
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
            LocationZoneId::new("")
        }
    }
}
