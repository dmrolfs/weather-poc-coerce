use super::state::UpdateLocationsState;
use super::{UpdateLocationsCommand, UpdateLocationsEvent};
use crate::model::update::UpdateLocationServicesRef;
use crate::model::LocationZoneCode;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::LocalActorRef;
use coerce::persistent::types::JournalTypes;
use coerce::persistent::{PersistentActor, Recover};
use coerce_cqrs::{AggregateState, ApplyAggregateEvent, CommandResult};
use tagid::{CuidId, Entity, Id, Label, Labeling};
use tracing::Instrument;

pub type UpdateLocationsSaga = LocalActorRef<UpdateLocations>;
pub type UpdateLocationsId = CuidId<UpdateLocations>;

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
        match command {
            UpdateLocationsCommand::UpdateLocations(zones) => {
                self.do_update_locations(zones.as_slice(), ctx);
            },
        }
    }

    fn do_update_locations(&self, zones: &[LocationZoneCode], ctx: &ActorContext) {
        tokio::spawn(
            async {
                debug!(
                    "DMR: Saga[{}] starting to update zones: {zones:?}",
                    ctx.id()
                );
                let saga_id: UpdateLocationsId =
                    Id::direct(UpdateLocations::labeler().label(), ctx.id().to_string());
                self.services.add_subscriber(saga_id, zones).await;
            }
            .instrument(debug_span!("add_saga_subscriber", saga_id=%ctx.id(), )),
        );
    }
}

#[async_trait]
impl PersistentActor for UpdateLocations {
    #[instrument(level = "debug", skip(journal))]
    fn configure(journal: &mut JournalTypes<Self>) {
        journal.message::<UpdateLocationsEvent>("update-locations-event");
    }
}

#[async_trait]
impl Handler<UpdateLocationsCommand> for UpdateLocations {
    #[instrument(level = "debug", skip(ctx))]
    async fn handle(
        &mut self, command: UpdateLocationsCommand, ctx: &mut ActorContext,
    ) -> <UpdateLocationsCommand as Message>::Result {
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
        }

        self.then_run(command, ctx);

        CommandResult::ok(())
    }
}

impl ApplyAggregateEvent<UpdateLocationsEvent> for UpdateLocations {
    type BaseType = Self;

    fn apply_event(
        &mut self, event: UpdateLocationsEvent, ctx: &mut ActorContext,
    ) -> Option<Self::BaseType> {
        if let Some(new_state) = self.state.apply_event(event, ctx) {
            self.state = new_state;
        }

        None
    }
}

#[async_trait]
impl Recover<UpdateLocationsEvent> for UpdateLocations {
    #[instrument(level = "debug", skip(ctx))]
    async fn recover(&mut self, event: UpdateLocationsEvent, ctx: &mut ActorContext) {
        info!("[{}] RECOVERING from EVENT: {event:?}", ctx.id());
        if let Some(new_type) = self.apply_event(event, ctx) {
            *self = new_type;
        }
    }
}
