use std::collections::HashSet;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::persistent::{PersistentActor, Recover};
use coerce::persistent::types::JournalTypes;
use coerce_cqrs::{AggregateState, CommandResult};
use once_cell::sync::OnceCell;
use tagid::{CuidGenerator, Entity, Id, IdGenerator, Label};
use tokio::task::JoinHandle;
use crate::model::LocationZoneCode;
use protocol::{RegistrarCommand, RegistrarEvent};
use crate::model::registrar::errors::RegistrarError;
use tracing::Instrument;
use crate::model::registrar::services::{RegistrarApi, RegistrarServices};

pub type RegistrarAggregate = coerce::actor::LocalActorRef<Registrar>;

static SERVICES: OnceCell<RegistrarServices> = OnceCell::new();

#[derive(Debug, Default, Clone, Label, Serialize, Deserialize)]
pub struct Registrar {
    location_codes: HashSet<LocationZoneCode>,
}

impl Registrar {
    /// Initializes the `RegistrarServices` used by the Registrar actor. This may be initialized
    /// once, and will return the supplied value in an Err (i.e., `Err(services)`) on subsequent
    /// calls.
    pub fn initialize_services(services: RegistrarServices) -> Result<(), RegistrarServices> {
        SERVICES.set(services)
    }

    fn services() -> &'static RegistrarServices {
        SERVICES.get().expect("RegistrationServices are not initialized")
    }
}

impl Entity for Registrar {
    type IdGen = CuidGenerator;

    fn next_id() -> Id<Self, <Self::IdGen as IdGenerator>::IdType> {
        static ID: OnceCell<Id<Registrar, <CuidGenerator as IdGenerator>::IdType>> = OnceCell::new();
        ID.get_or_init(|| Id::new()).clone()
    }
}

#[async_trait]
impl PersistentActor for Registrar {
    #[instrument(level="debug", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal
            .message::<RegistrarEvent>("registrar-event");
    }
}


impl AggregateState<RegistrarCommand, RegistrarEvent> for Registrar {
    type Error = RegistrarError;
    type State = HashSet<LocationZoneCode>;

    #[instrument(level="debug", skip(_ctx))]
    fn handle_command(&self, command: RegistrarCommand, _ctx: &mut ActorContext) -> Result<Vec<RegistrarEvent>, Self::Error> {
        match command {
            RegistrarCommand::UpdateWeather => Ok(vec![]),
            RegistrarCommand::MonitorForecastZone(zone) if !self.location_codes.contains(&zone) => {
                Ok(vec![RegistrarEvent::ForecastZoneAdded(zone)])
            },
            RegistrarCommand::MonitorForecastZone(zone) => Err(RegistrarError::RejectedCommand(
                format!("already monitoring location zone code: {zone}"),
            )),
            RegistrarCommand::ClearZoneMonitoring => {
                Ok(vec![RegistrarEvent::AllForecastZonesForgotten])
            },
            RegistrarCommand::ForgetForecastZone(zone) => {
                Ok(vec![RegistrarEvent::ForecastZoneForgotten(zone)])
            },
        }
    }

    #[instrument(level="debug", skip(_ctx))]
    fn apply_event(&mut self, event: RegistrarEvent, _ctx: &mut ActorContext) -> Option<Self::State> {
        match event {
            RegistrarEvent::ForecastZoneAdded(zone) => {
                self.location_codes.insert(zone);
            },
            RegistrarEvent::ForecastZoneForgotten(zone) => {
                self.location_codes.remove(&zone);
            },
            RegistrarEvent::AllForecastZonesForgotten => {
                self.location_codes.clear();
            },
        }
        None // since state is simple handle here and avoid extra cloning.
    }
}

#[async_trait]
impl Handler<RegistrarCommand> for Registrar {
    #[instrument(level="debug", skip(ctx))]
    async fn handle(&mut self, command: RegistrarCommand, ctx: &mut ActorContext) -> <RegistrarCommand as Message>::Result {
        let events = match self.handle_command(command.clone(), ctx) {
            Ok(events) => events,
            Err(RegistrarError::RejectedCommand(msg)) => return CommandResult::Rejected(msg),
            Err(error) => return error.into(),
        };

        debug!("[{}] RESULTING EVENTS: {events:?}", ctx.id());
        for ref event in events {
            debug!("[{}] PERSISTING event: {event:?}", ctx.id());
            if let Err(error) = self.persist(event, ctx).await {
                error!(?error, "[{}] failed to persist event: {error:?}", ctx.id());
                return error.into();
            }

            debug!("[{}] APPLYING event: {event:?}", ctx.id());
            let _ignored = self.apply_event(event.clone(), ctx);
        }


        self.then_run(command, events, ctx);
        CommandResult::Ok(())
    }
}

impl Registrar {
    fn then_run(&self, command: RegistrarCommand, events: Vec<RegistrarEvent>, ctx: &ActorContext) -> JoinHandle<()> {
        let result = tokio::spawn(
            async {
                match command {
                    RegistrarCommand::UpdateWeather => {
                        let loc_codes: Vec<_> = self.location_codes.iter().collect();
                        if let Err(error) = Self::services().update_weather(&loc_codes, ctx).await {
                            error!(?error, "update weather service failed for location codes: {loc_codes:?}");
                        }
                    },

                    RegistrarCommand::MonitorForecastZone(zone) if !self.location_codes.contains(&zone) => {
                        if let Err(error) = Self::services().initialize_forecast_zone(&zone, ctx).await {
                            error!(?error, "failed to initialize forecast zone: {zone:?}");
                        }
                    },

                    _ => {},
                }
            }
                .instrument(debug_span!("registrar command service", ?command))
        );

        result
    }
}

#[async_trait]
impl Recover<RegistrarEvent> for Registrar {
    #[instrument(level="debug", skip(ctx))]
    async fn recover(&mut self, event: RegistrarEvent, ctx: &mut ActorContext) {
        if let Some(new_locations) = self.apply_event(event, ctx) {
            self.location_codes = new_locations;
        }
    }
}

mod services {
    use coerce::actor::context::ActorContext;
    use coerce::actor::IntoActor;
    use crate::model::{LocationZone, LocationZoneCode};
    use crate::model::registrar::errors::RegistrarError;
    use crate::model::zone::{LocationServicesRef, LocationZoneAggregate, LocationZoneCommand};

    #[async_trait]
    pub trait RegistrarApi: Sync + Send {
        async fn initialize_forecast_zone(&self, zone: &LocationZoneCode, ctx: &ActorContext) -> Result<(), RegistrarError>;

        async fn update_weather(&self, zones: &[&LocationZoneCode], ctx: &ActorContext) -> Result<(), RegistrarError>;
    }

    #[derive(Debug, Clone, )]
    pub enum RegistrarServices {
        Full(FullRegistrarServices),
        HappyPath(HappyPathServices),
    }

    impl RegistrarServices {
        pub const fn full(location_services: LocationServicesRef) -> Self {
            Self::Full(FullRegistrarServices::new(location_services))
        }

        pub const fn happy() -> Self { Self::HappyPath(HappyPathServices)}
    }

    #[async_trait]
    impl RegistrarApi for RegistrarServices {
        async fn initialize_forecast_zone(&self, zone: &LocationZoneCode, ctx: &ActorContext) -> Result<(), RegistrarError> {
            match self  {
                Self::Full(svc) => svc.initialize_forecast_zone(zone, ctx).await,
                Self::HappyPath(svc) => svc.initialize_forecast_zone(zone, ctx).await,
            }
        }

        async fn update_weather(&self, zones: &[&LocationZoneCode], ctx: &ActorContext) -> Result<(), RegistrarError> {
            match self  {
                Self::Full(svc) => svc.update_weather(zones, ctx).await,
                Self::HappyPath(svc) => svc.update_weather(zones, ctx).await,
            }
        }
    }

    #[derive(Debug, Clone,)]
    pub struct FullRegistrarServices {
        location_services: LocationServicesRef,
    }

    impl FullRegistrarServices {
        pub const fn new(location_services: LocationServicesRef) -> Self {
            Self { location_services, }
        }

        pub async fn location_zone_for(&self, zone: &LocationZoneCode, ctx: &ActorContext) -> Result<LocationZoneAggregate, RegistrarError> {
            let aggregate_id = zone.to_string();
            let aggregate = LocationZone::new(self.location_services.clone())
                .into_actor(Some(aggregate_id), ctx.system())
                .await?;
            Ok(aggregate)
        }
    }

    #[async_trait]
    impl RegistrarApi for FullRegistrarServices {
        async fn initialize_forecast_zone(&self, zone: &LocationZoneCode, ctx: &ActorContext) -> Result<(), RegistrarError> {
            let location_actor = self.location_zone_for(zone, ctx).await?;
            location_actor.notify(LocationZoneCommand::Subscribe(zone.clone()))?;
            Ok(())
        }

        #[instrument(level="debug", skip(self, ctx))]
        async fn update_weather(&self, zones: &[&LocationZoneCode], ctx: &ActorContext) -> Result<(), RegistrarError> {
            if zones.is_empty() {
                return Ok(());
            }

            let zone_ids = zones.iter().copied().cloned().collect();
            let saga_id = crate::model::update::generate_id();
            let update_saga = UpdateLocationsSaga::default().into_actor(Some(saga_id), ctx.system()).await?;
            debug!("DMR: Update Locations saga identifier: {saga_id:?}");
            // let metadata = maplit::hashmap! { "correlation".to_string() => saga_id.id.to_string(), };
            update_saga.notify(UpdateLocationsCommand::UpdateLocations(saga_id.clone(), zone_ids))?;
            Ok(())
        }
    }

    #[derive(Debug, Copy, Clone, Serialize, Deserialize)]
    pub struct HappyPathServices;

    #[async_trait]
    impl RegistrarApi for HappyPathServices {
        #[instrument(level="debug", skip(_ctx))]
        async fn initialize_forecast_zone(&self, _zone: &LocationZoneCode, _ctx: &ActorContext) -> Result<(), RegistrarError> {
            Ok(())
        }

        #[instrument(level="debug", skip(_ctx))]
        async fn update_weather(&self, _zones: &[&LocationZoneCode], _ctx: &ActorContext) -> Result<(), RegistrarError> {
            Ok(())
        }
    }
}

mod protocol {
    use strum_macros::Display;
    use crate::model::LocationZoneCode;
    use coerce_cqrs::CommandResult;

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