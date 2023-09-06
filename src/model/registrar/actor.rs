use super::errors::RegistrarError;
use super::protocol::{RegistrarCommand, RegistrarEvent};
use super::services::{RegistrarApi, RegistrarServices};
use crate::model::LocationZoneCode;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::persistent::types::JournalTypes;
use coerce::persistent::{PersistentActor, Recover};
use coerce_cqrs::{AggregateState, CommandResult};
use once_cell::sync::OnceCell;
use std::collections::HashSet;
use tagid::{CuidGenerator, Entity, Id, IdGenerator, Label};
use tokio::task::JoinHandle;
use tracing::Instrument;

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
        static ID: OnceCell<Id<Registrar, <CuidGenerator as IdGenerator>::IdType>> =
            OnceCell::new();
        ID.get_or_init(|| Id::new()).clone()
    }
}

#[async_trait]
impl PersistentActor for Registrar {
    #[instrument(level = "debug", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal.message::<RegistrarEvent>("registrar-event");
    }
}

impl AggregateState<RegistrarCommand, RegistrarEvent> for Registrar {
    type Error = RegistrarError;
    type State = HashSet<LocationZoneCode>;

    #[instrument(level = "debug", skip(_ctx))]
    fn handle_command(
        &self, command: RegistrarCommand, _ctx: &mut ActorContext,
    ) -> Result<Vec<RegistrarEvent>, Self::Error> {
        use RegistrarCommand as C;
        use RegistrarEvent as E;

        match command {
            C::UpdateWeather => Ok(vec![]),
            C::MonitorForecastZone(zone) if !self.location_codes.contains(&zone) => {
                Ok(vec![E::ForecastZoneAdded(zone)])
            },
            C::MonitorForecastZone(zone) => Err(RegistrarError::RejectedCommand(format!(
                "already monitoring location zone code: {zone}"
            ))),
            C::ClearZoneMonitoring => Ok(vec![E::AllForecastZonesForgotten]),
            C::ForgetForecastZone(zone) => Ok(vec![E::ForecastZoneForgotten(zone)]),
        }
    }

    #[instrument(level = "debug", skip(_ctx))]
    fn apply_event(
        &mut self, event: RegistrarEvent, _ctx: &mut ActorContext,
    ) -> Option<Self::State> {
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
}

#[async_trait]
impl Handler<RegistrarCommand> for Registrar {
    #[instrument(level = "debug", skip(ctx))]
    async fn handle(
        &mut self, command: RegistrarCommand, ctx: &mut ActorContext,
    ) -> <RegistrarCommand as Message>::Result {
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
    fn then_run(
        &self, command: RegistrarCommand, _events: Vec<RegistrarEvent>, ctx: &ActorContext,
    ) -> JoinHandle<()> {
        let result = tokio::spawn(
            async {
                match command {
                    RegistrarCommand::UpdateWeather => {
                        let loc_codes: Vec<_> = self.location_codes.iter().collect();
                        if let Err(error) = Self::services().update_weather(&loc_codes, ctx).await {
                            error!(
                                ?error,
                                "update weather service failed for location codes: {loc_codes:?}"
                            );
                        }
                    },

                    RegistrarCommand::MonitorForecastZone(zone)
                        if !self.location_codes.contains(&zone) =>
                    {
                        if let Err(error) =
                            Self::services().initialize_forecast_zone(&zone, ctx).await
                        {
                            error!(?error, "failed to initialize forecast zone: {zone:?}");
                        }
                    },

                    _ => {},
                }
            }
            .instrument(debug_span!("registrar command service", ?command)),
        );

        result
    }
}

#[async_trait]
impl Recover<RegistrarEvent> for Registrar {
    #[instrument(level = "debug", skip(ctx))]
    async fn recover(&mut self, event: RegistrarEvent, ctx: &mut ActorContext) {
        if let Some(new_locations) = self.apply_event(event, ctx) {
            self.location_codes = new_locations;
        }
    }
}
