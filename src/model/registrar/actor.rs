use super::errors::RegistrarError;
use super::protocol::{RegistrarAdminCommand, RegistrarEvent, UpdateWeather};
use super::services::{RegistrarApi, RegistrarServices};
use crate::model::registrar::view::{MONITORED_ZONES_TABLE, REGISTRAR_OFFSET_TABLE};
use crate::model::registrar::{
    services, MonitoredZonesView, RegistrarAggregateSupport, RegistrarServicesRef,
    MONITORED_ZONES_VIEW,
};
use crate::model::LocationZoneCode;
use crate::{settings, Settings};
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::system::ActorSystem;
use coerce::persistent::types::JournalTypes;
use coerce::persistent::{PersistentActor, Recover};
use coerce_cqrs::postgres::PostgresProjectionStorage;
use coerce_cqrs::projection::processor::ProcessorSourceRef;
use coerce_cqrs::{AggregateState, CommandResult};
use once_cell::sync::OnceCell;
use smol_str::SmolStr;
use std::collections::HashSet;
use std::sync::Arc;
use tagid::{Entity, Id, IdGenerator, Label};
use tracing::Instrument;

pub type RegistrarAggregate = coerce::actor::LocalActorRef<Registrar>;

pub type RegistrarId = Id<Registrar, <<Registrar as Entity>::IdGen as IdGenerator>::IdType>;

static SINGLETON_ID: OnceCell<RegistrarId> = OnceCell::new();

#[inline]
pub fn singleton_id() -> &'static RegistrarId {
    SINGLETON_ID.get_or_init(Registrar::next_id)
}

#[inline]
pub async fn registrar_actor(system: &ActorSystem) -> Result<RegistrarAggregate, RegistrarError> {
    use coerce::actor::{IntoActor, IntoActorId};

    let id = singleton_id().clone().into_actor_id();
    match system.get_tracked_actor(id).await {
        Some(actor_ref) => Ok(actor_ref),
        None => {
            let id = singleton_id().into_actor_id();
            let registrar = Registrar {
                location_codes: HashSet::default(),
                services: services::services(),
            };
            let aggregate = registrar.into_actor(Some(id), system).await?;
            Ok(aggregate)
        },
    }
}

#[derive(Debug, Clone, Label)]
pub struct Registrar {
    location_codes: HashSet<LocationZoneCode>,
    services: RegistrarServicesRef,
}

impl Registrar {
    pub async fn initialize_aggregate_support(
        journal_storage: ProcessorSourceRef, services: RegistrarServices, settings: &Settings,
        system: &ActorSystem,
    ) -> Result<RegistrarAggregateSupport, RegistrarError> {
        let storage_config = settings::storage_config_from(&settings.database, &settings.registrar);
        let monitored_zones_storage = PostgresProjectionStorage::<MonitoredZonesView>::new(
            MONITORED_ZONES_VIEW,
            Some(MONITORED_ZONES_TABLE.clone()),
            REGISTRAR_OFFSET_TABLE.clone(),
            &storage_config,
            system,
        )
        .await?;
        let monitored_zones_projection = Arc::new(monitored_zones_storage);
        let monitored_zones_processor = support::monitored_zones_processor(
            journal_storage,
            monitored_zones_projection.clone(),
            system,
        )?;

        let services = Arc::new(services);
        if let Err(svc) = super::services::initialize_services(services.clone()) {
            warn!(extra_service=?svc, "attempt to reinitialize RegistrarServices - ignored");
        }

        Ok(RegistrarAggregateSupport {
            monitored_zones_processor,
            monitored_zones_projection,
            services,
        })
    }
}

pub struct SingletonIdGenerator;

const REGISTRAR_SINGLETON_ID: &str = "<singleton>";

impl IdGenerator for SingletonIdGenerator {
    type IdType = SmolStr;

    fn next_id_rep() -> Self::IdType {
        SmolStr::new(REGISTRAR_SINGLETON_ID)
    }
}

impl Entity for Registrar {
    type IdGen = SingletonIdGenerator;
}

#[async_trait]
impl PersistentActor for Registrar {
    #[instrument(level = "debug", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal.message::<RegistrarEvent>("registrar-event");
    }
}

impl AggregateState<RegistrarAdminCommand, RegistrarEvent> for Registrar {
    type Error = RegistrarError;
    type State = HashSet<LocationZoneCode>;

    #[instrument(level = "debug")]
    fn handle_command(
        &self, command: &RegistrarAdminCommand,
    ) -> CommandResult<Vec<RegistrarEvent>, Self::Error> {
        use RegistrarAdminCommand as C;
        use RegistrarEvent as E;

        match command {
            C::MonitorForecastZone(zone) if !self.location_codes.contains(zone) => {
                CommandResult::ok(vec![E::ForecastZoneAdded(zone.clone())])
            },
            C::MonitorForecastZone(zone) => {
                CommandResult::rejected(format!("already monitoring location zone code: {zone}"))
            },
            C::ClearZoneMonitoring => CommandResult::ok(vec![E::AllForecastZonesForgotten]),
            C::ForgetForecastZone(zone) => {
                CommandResult::ok(vec![E::ForecastZoneForgotten(zone.clone())])
            },
        }
    }

    #[instrument(level = "debug")]
    fn apply_event(&mut self, event: RegistrarEvent) -> Option<Self::State> {
        use RegistrarEvent as E;

        match event {
            E::ForecastZoneAdded(zone) => {
                self.location_codes.insert(zone);
            },
            E::ForecastZoneForgotten(zone) => {
                self.location_codes.remove(&zone);
            },
            E::AllForecastZonesForgotten => {
                self.location_codes.clear();
            },
        }

        None // since state is simple handle here and avoid extra cloning.
    }

    #[instrument(level = "debug", skip(ctx))]
    fn then_run(&self, command: &RegistrarAdminCommand, ctx: &ActorContext) {
        let system = ctx.system().clone();
        if let RegistrarAdminCommand::MonitorForecastZone(zone) = command {
            if !self.location_codes.contains(zone) {
                let zone = zone.clone();
                let services = self.services.clone();

                tokio::spawn(
                    async move {
                        let outcome = services.initialize_forecast_zone(&zone, &system).await;
                        if let Err(error) = outcome {
                            error!(?error, "failed to initialize forecast zone: {zone:?}");
                        }
                    }
                    .instrument(debug_span!("registrar command service", ?command)),
                );
            }
        }
    }
}

#[async_trait]
impl Handler<UpdateWeather> for Registrar {
    #[instrument(level = "debug", skip(ctx))]
    async fn handle(
        &mut self, _: UpdateWeather, ctx: &mut ActorContext,
    ) -> <UpdateWeather as Message>::Result {
        let zones: Vec<_> = self.location_codes.iter().collect();
        match self.services.update_weather(&zones, ctx).await {
            Ok(saga_id) => CommandResult::Ok(saga_id),
            Err(error) => {
                error!(
                    ?error,
                    "update weather service failed for location zones: {zones:?}"
                );
                CommandResult::Err(error.into())
            },
        }
    }
}

#[async_trait]
impl Handler<RegistrarAdminCommand> for Registrar {
    #[instrument(level = "debug", skip(ctx))]
    async fn handle(
        &mut self, command: RegistrarAdminCommand, ctx: &mut ActorContext,
    ) -> <RegistrarAdminCommand as Message>::Result {
        let events = match self.handle_command(&command) {
            CommandResult::Ok(events) => events,
            CommandResult::Rejected(msg) => return CommandResult::rejected(msg),
            CommandResult::Err(error) => {
                error!(?command, ?error, "Registrar command failed.");
                return CommandResult::err(error.into());
            },
        };

        debug!("[{}] RESULTING EVENTS: {events:?}", ctx.id());
        for ref event in events {
            debug!("[{}] PERSISTING event: {event:?}", ctx.id());
            if let Err(error) = self.persist(event, ctx).await {
                error!(?error, "[{}] failed to persist event: {error:?}", ctx.id());
                return CommandResult::err(error.into());
            }

            debug!("[{}] APPLYING event: {event:?}", ctx.id());
            let _ignored = self.apply_event(event.clone());
        }

        self.then_run(&command, ctx);

        CommandResult::ok(())
    }
}

#[async_trait]
impl Recover<RegistrarEvent> for Registrar {
    #[instrument(level = "debug", skip(_ctx))]
    async fn recover(&mut self, event: RegistrarEvent, _ctx: &mut ActorContext) {
        if let Some(new_locations) = self.apply_event(event) {
            self.location_codes = new_locations;
        }
    }
}

pub mod support {
    use crate::model::registrar::services::RegistrarServicesRef;
    use crate::model::registrar::{
        MonitoredZonesProjection, MonitoredZonesView, Registrar, MONITORED_ZONES_VIEW,
    };
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
    pub struct RegistrarAggregateSupport {
        pub monitored_zones_processor: ProcessorEngineRef,
        pub monitored_zones_projection: MonitoredZonesProjection,
        pub services: RegistrarServicesRef,
    }

    static MONITORED_ZONES_PROCESSOR: OnceCell<ProcessorEngineRef> = OnceCell::new();
    pub fn monitored_zones_processor(
        journal_storage: ProcessorSourceRef,
        view_storage: ProjectionStorageRef<PersistenceId, MonitoredZonesView>,
        system: &ActorSystem,
    ) -> Result<ProcessorEngineRef, ProjectionError> {
        let processor = MONITORED_ZONES_PROCESSOR.get_or_try_init(|| {
            let monitored_zones_apply = ProjectionApplicator::new(MonitoredZonesView::apply_event);

            Processor::builder_for::<Registrar, _, _, _, _>(MONITORED_ZONES_VIEW)
                .with_entry_handler(monitored_zones_apply)
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
