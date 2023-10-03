use super::state::UpdateLocationsState;
use super::{UpdateLocationsCommand, UpdateLocationsEvent};
use crate::model::update::{
    services, UpdateLocationServices, UpdateLocationServicesRef, UpdateLocationsAggregateSupport,
    UpdateLocationsError,
};
use crate::model::LocationZoneCode;
use crate::services::noaa::{NoaaWeatherApi, NoaaWeatherServices};
use crate::Settings;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::system::ActorSystem;
use coerce::actor::{IntoActor, LocalActorRef};
use coerce::persistent::types::JournalTypes;
use coerce::persistent::{PersistentActor, Recover};
use coerce_cqrs::projection::processor::ProcessorSourceRef;
use coerce_cqrs::{Aggregate, AggregateState, CommandResult};
use std::str::FromStr;
use std::sync::Arc;
use tagid::{CuidId, Entity, Id, Label, Labeling};
use tracing::Instrument;
use url::Url;

#[allow(dead_code)]
pub type UpdateLocationsSaga = LocalActorRef<UpdateLocations>;
pub type UpdateLocationsId = CuidId<UpdateLocations>;

pub async fn update_locations_saga(
    system: &ActorSystem,
) -> Result<(UpdateLocationsId, UpdateLocationsSaga), UpdateLocationsError> {
    let saga_id = super::generate_id();
    let services = services::services();
    let saga = UpdateLocations::new(services)
        .into_actor(Some(saga_id.clone()), system)
        .await?;
    Ok((saga_id, saga))
}

#[derive(Debug, Clone, Label)]
pub struct UpdateLocations {
    state: UpdateLocationsState,
    services: UpdateLocationServicesRef,
}

impl UpdateLocations {
    pub fn new(services: UpdateLocationServicesRef) -> Self {
        Self { state: UpdateLocationsState::default(), services }
    }
}

impl UpdateLocations {
    pub async fn initialize_aggregate_support(
        journal_processor_source: ProcessorSourceRef, location_zone_source: ProcessorSourceRef,
        settings: &Settings, system: &ActorSystem,
    ) -> Result<UpdateLocationsAggregateSupport, UpdateLocationsError> {
        let (update_history_processor, update_history_projection) =
            support::start_update_locations_history_processor(
                journal_processor_source.clone(),
                settings,
                system,
            )
            .await?;

        //subscription
        let location_zone_subscription_support =
            support::start_location_zone_subscription(location_zone_source, settings, system)
                .await?;
        let location_subscription_channel_id =
            location_zone_subscription_support.subscription_channel_actor_id.clone();

        // services
        let user_agent = axum::http::HeaderValue::from_str("(here.com, contact@example.com)")
            .expect("invalid user_agent");
        let base_url = Url::from_str("https://api.weather.gov")?;
        let noaa_api = NoaaWeatherApi::new(base_url, user_agent)?;
        let noaa = NoaaWeatherServices::Noaa(noaa_api);
        let mut update_services = Arc::new(UpdateLocationServices::new(
            noaa,
            location_subscription_channel_id,
            system.clone(),
        ));
        if let Err(svc) = services::initialize_services(update_services.clone()) {
            warn!(extra_service=?svc, "attempt to reinitialize UpdateLocationServices - ignored");
            update_services = services::services();
        }

        // controller
        let controller_processor = support::start_controller_processor(
            journal_processor_source,
            settings,
            update_services,
            system,
        )
        .await?;

        Ok(UpdateLocationsAggregateSupport {
            update_history_processor,
            update_history_projection,
            controller_processor,
            location_zone_subscription_support,
        })
    }
}

impl PartialEq for UpdateLocations {
    fn eq(&self, other: &Self) -> bool {
        self.state == other.state
    }
}

impl Entity for UpdateLocations {
    type IdGen = tagid::CuidGenerator;
}

impl UpdateLocations {
    #[instrument(level = "debug", skip(self, ctx))]
    fn then_run(&self, command: UpdateLocationsCommand, ctx: &ActorContext) {
        use UpdateLocationsCommand as C;

        if let C::UpdateLocations(zones) = command {
            self.do_update_locations(zones, ctx);
        }
    }

    fn do_update_locations(&self, zones: Vec<LocationZoneCode>, ctx: &ActorContext) {
        let services = self.services.clone();
        let saga_id = ctx.id().clone();
        let id = saga_id.clone();
        let zones_0 = zones.clone();
        tokio::spawn(
            async move {
                debug!("DMR: Saga[{id}] subscribing update process to zones events: {zones:?}");
                let saga_id: UpdateLocationsId =
                    Id::direct(UpdateLocations::labeler().label(), id.to_string());
                if let Err(error) = services.add_subscriber(saga_id, &zones).await {
                    error!(?error, "failed to register update locations saga, {id}, for location event broadcasts.");
                }
            }
            .instrument(debug_span!("subscribe update saga to zone(s) events", %saga_id, zones=?zones_0)),
        );
    }
}

impl Aggregate for UpdateLocations {}

#[async_trait]
impl PersistentActor for UpdateLocations {
    #[instrument(level = "debug", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal.message::<UpdateLocationsEvent>(&Self::journal_message_type_identifier::<
            UpdateLocationsEvent,
        >());
    }
}

#[async_trait]
impl Handler<UpdateLocationsCommand> for UpdateLocations {
    #[instrument(level = "debug", skip(ctx))]
    async fn handle(
        &mut self, command: UpdateLocationsCommand, ctx: &mut ActorContext,
    ) -> <UpdateLocationsCommand as Message>::Result {
        let events = match self.state.handle_command(&command) {
            CommandResult::Ok(events) => events,
            CommandResult::Rejected(msg) => return CommandResult::rejected(msg),
            CommandResult::Err(error) => return CommandResult::err(error.into()),
        };

        debug!("[{}] RESULTING EVENTS: {events:?}", ctx.id());
        for event in events {
            debug!("[{}] PERSISTING event: {event:?}", ctx.id());
            if let Err(error) = self.persist(&event, ctx).await {
                error!(?event, "[{}] failed to persist event: {error:?}", ctx.id());
                return CommandResult::err(error.into());
            }

            debug!("[{}] APPLYING event: {event:?}", ctx.id());
            if let Some(new_state) = self.state.apply_event(event) {
                self.state = new_state;
            }
        }

        self.then_run(command, ctx);

        CommandResult::ok(())
    }
}

#[async_trait]
impl Recover<UpdateLocationsEvent> for UpdateLocations {
    #[instrument(level = "debug", skip(ctx))]
    async fn recover(&mut self, event: UpdateLocationsEvent, ctx: &mut ActorContext) {
        info!("[{}] RECOVERING from EVENT: {event:?}", ctx.id());
        if let Some(new_type) = self.state.apply_event(event) {
            self.state = new_type;
        }
    }
}

pub mod support {
    use crate::connect::{
        ConnectError, EventCommandTopic, EventSubscriptionChannelRef, EventSubscriptionProcessor,
    };
    use crate::model::update::controller::{
        UpdateLocationsController, UPDATE_LOCATIONS_CONTROLLER,
    };
    use crate::model::update::view::{
        UPDATE_LOCATIONS_HISTORY_TABLE, UPDATE_LOCATIONS_HISTORY_VIEW,
        UPDATE_LOCATIONS_OFFSET_TABLE,
    };
    use crate::model::update::{
        LocationZoneBroadcastTopic, UpdateLocationServicesRef, UpdateLocationsHistory,
        UpdateLocationsHistoryProjection, UPDATE_LOCATION_ZONE_SUBSCRIPTION,
        UPDATE_LOCATION_ZONE_SUBSCRIPTION_OFFSET_TABLE,
    };
    use crate::model::{LocationZone, UpdateLocations};
    use crate::{settings, Settings};
    use coerce::actor::system::ActorSystem;
    use coerce::actor::{ActorId, ActorRefErr};
    use coerce_cqrs::postgres::{
        BinaryProjection, PostgresProjectionStorage, PostgresStorageConfig, TableName,
    };
    use coerce_cqrs::projection::processor::{
        Processor, ProcessorEngine, ProcessorEngineRef, ProcessorSourceRef, Ready, RegularInterval,
    };
    use coerce_cqrs::projection::{
        PersistenceId, ProjectionApplicator, ProjectionError, ProjectionStorageRef,
    };
    use std::fmt::{self, Debug};
    use std::marker::PhantomData;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Clone)]
    pub struct UpdateLocationsAggregateSupport {
        pub update_history_processor: ProcessorEngineRef,
        pub update_history_projection: UpdateLocationsHistoryProjection,
        pub controller_processor: ProcessorEngineRef,
        pub location_zone_subscription_support: SubscriptionSupport<LocationZoneBroadcastTopic>,
    }

    impl Debug for UpdateLocationsAggregateSupport {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("UpdateLocationsAggregateSupport")
                .field("update_history_processor", &self.update_history_processor)
                .field(
                    "update_history_projection",
                    &self.update_history_projection.name(),
                )
                .field("controller_processor", &self.controller_processor)
                .field(
                    "location_zone_subscription_support",
                    &self.location_zone_subscription_support,
                )
                .finish()
        }
    }

    #[derive(Debug, Clone)]
    pub struct SubscriptionSupport<T: EventCommandTopic> {
        pub subscription_processor: ProcessorEngineRef,
        pub subscription_channel_actor_id: ActorId,
        _marker: PhantomData<fn() -> T>,
    }

    impl<T: EventCommandTopic> SubscriptionSupport<T> {
        pub const fn new(
            subscription_processor: ProcessorEngineRef, subscription_channel_actor_id: ActorId,
        ) -> Self {
            Self {
                subscription_processor,
                subscription_channel_actor_id,
                _marker: PhantomData,
            }
        }

        #[inline]
        pub async fn channel_ref(
            &self, system: &ActorSystem,
        ) -> Result<EventSubscriptionChannelRef<T>, ConnectError> {
            system
                .get_tracked_actor(self.subscription_channel_actor_id.clone())
                .await
                .ok_or_else(|| ActorRefErr::NotFound(self.subscription_channel_actor_id.clone()))
                .map_err(|err| err.into())
        }
    }

    #[instrument(level = "debug", skip(journal_storage, settings, system))]
    pub async fn start_update_locations_history_processor(
        journal_storage: ProcessorSourceRef, settings: &Settings, system: &ActorSystem,
    ) -> Result<(ProcessorEngineRef, UpdateLocationsHistoryProjection), ProjectionError> {
        let update_storage_config =
            settings::storage_config_from(&settings.database, &settings.update_locations);
        let history_storage = projection_storage_from(
            UPDATE_LOCATIONS_HISTORY_VIEW,
            Some(UPDATE_LOCATIONS_HISTORY_TABLE.clone()),
            UPDATE_LOCATIONS_OFFSET_TABLE.clone(),
            update_storage_config,
            system,
        )
        .await?;
        let update_history_projection = history_storage.clone();
        let update_history_apply = ProjectionApplicator::<UpdateLocations, _, _, _>::new(
            UpdateLocationsHistory::apply_event,
        );

        let interval = Duration::from_secs(60);
        let engine: ProcessorEngine<Ready<PersistenceId, UpdateLocationsHistory, _, _>> =
            Processor::builder_for::<UpdateLocations, _, _, _, _>(UPDATE_LOCATIONS_HISTORY_VIEW)
                .with_entry_handler(update_history_apply)
                .with_system(system.clone())
                .with_source(journal_storage.clone())
                .with_projection_storage(history_storage)
                .with_interval_calculator(RegularInterval::of_duration(interval))
                .finish()?;

        let processor = engine.run()?;

        Ok((Arc::new(processor), update_history_projection))
    }

    #[instrument(level = "debug", skip(journal_storage, settings, system))]
    pub async fn start_controller_processor(
        journal_storage: ProcessorSourceRef, settings: &Settings,
        services: UpdateLocationServicesRef, system: &ActorSystem,
    ) -> Result<ProcessorEngineRef, ProjectionError> {
        let update_storage_config =
            settings::storage_config_from(&settings.database, &settings.update_locations);
        let storage = projection_storage_from(
            UPDATE_LOCATIONS_CONTROLLER,
            None,
            UPDATE_LOCATIONS_OFFSET_TABLE.clone(),
            update_storage_config,
            system,
        )
        .await?;
        let controller_apply = UpdateLocationsController::new(system.clone(), services);

        let interval = Duration::from_secs(10);
        let engine: ProcessorEngine<Ready<PersistenceId, (), _, _>> =
            Processor::builder_for::<UpdateLocations, _, _, _, _>(UPDATE_LOCATIONS_CONTROLLER)
                .with_entry_handler(controller_apply)
                .with_system(system.clone())
                .with_source(journal_storage.clone())
                .with_projection_storage(storage)
                .with_interval_calculator(RegularInterval::of_duration(interval))
                .finish()?;

        let controller = engine.run()?;
        Ok(Arc::new(controller))
    }

    #[instrument(level = "debug", skip(location_zone_storage, settings, system))]
    pub async fn start_location_zone_subscription(
        location_zone_storage: ProcessorSourceRef, settings: &Settings, system: &ActorSystem,
    ) -> Result<SubscriptionSupport<LocationZoneBroadcastTopic>, ConnectError> {
        let update_storage_config =
            settings::storage_config_from(&settings.database, &settings.update_locations);
        let projection_storage = projection_storage_from(
            UPDATE_LOCATION_ZONE_SUBSCRIPTION,
            None,
            UPDATE_LOCATION_ZONE_SUBSCRIPTION_OFFSET_TABLE.clone(),
            update_storage_config,
            system,
        )
        .await?;
        let subscription_processor =
            EventSubscriptionProcessor::new(LocationZoneBroadcastTopic, system).await?;
        let subscription_channel_actor_id = subscription_processor.channel_actor_id();

        let interval = Duration::from_secs(60);
        let engine: ProcessorEngine<Ready<PersistenceId, (), _, _>> =
            Processor::builder_for::<LocationZone, _, _, _, _>(UPDATE_LOCATION_ZONE_SUBSCRIPTION)
                .with_entry_handler(subscription_processor)
                .with_system(system.clone())
                .with_source(location_zone_storage)
                .with_projection_storage(projection_storage)
                .with_interval_calculator(RegularInterval::of_duration(interval))
                .finish()
                .map_err(ProjectionError::Processor)?;

        let subscription = engine.run()?;
        Ok(SubscriptionSupport::new(
            Arc::new(subscription),
            subscription_channel_actor_id,
        ))
    }

    #[instrument(level = "debug", skip(config, system))]
    async fn projection_storage_from<V>(
        name: &str, view_storage_table: Option<TableName>, offset_table: TableName,
        config: PostgresStorageConfig, system: &ActorSystem,
    ) -> Result<ProjectionStorageRef<PersistenceId, V>, ProjectionError>
    where
        V: BinaryProjection + Debug + 'static,
    {
        let storage = PostgresProjectionStorage::<V>::new(
            name,
            view_storage_table.clone(),
            offset_table.clone(),
            &config,
            system
        )
            .await
            .map_err(|err| ProjectionError::Storage {
                cause: err.into(),
                meta: maplit::hashmap! {
                    "projection".to_string() => name.to_string(),
                    "view_storage_table".to_string() => view_storage_table.map(|rep| rep.to_string()).unwrap_or_else(|| "<none>".to_string()),
                    "offset_table".to_string() => offset_table.to_string(),
                }
            })?;

        Ok(Arc::new(storage))
    }
}
