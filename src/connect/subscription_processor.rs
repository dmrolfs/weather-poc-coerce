use crate::connect::{
    ConnectError, EventCommandTopic, EventEnvelope, EventSubscriptionChannel,
    EventSubscriptionChannelRef,
};
use coerce::actor::system::ActorSystem;
use coerce::actor::{ActorId, ActorRefErr, IntoActor, IntoActorId};
use coerce::persistent::storage::JournalEntry;
use coerce_cqrs::projection::processor::{ProcessEntry, ProcessResult, ProcessorContext};
use coerce_cqrs::projection::ProjectionError;
use std::fmt;
use tagid::{Entity, Id, Label};

#[derive(Clone)]
pub struct EventSubscriptionProcessor<T: EventCommandTopic> {
    channel_ref: EventSubscriptionChannelRef<T>,
    channel_id: Id<EventSubscriptionChannel<T>, String>,
}

impl<T: EventCommandTopic + Label> EventSubscriptionProcessor<T> {
    pub async fn new(topic: T, system: &ActorSystem) -> Result<Self, ConnectError> {
        let id = EventSubscriptionChannel::<T>::next_id();
        let channel_ref = EventSubscriptionChannel::new(topic)
            .into_actor(Some(id.clone()), system)
            .await?;
        Ok(Self { channel_ref, channel_id: id })
    }

    #[allow(dead_code)]
    #[inline]
    pub fn channel_actor_id(&self) -> ActorId {
        self.channel_id.clone().into_actor_id()
    }

    #[allow(dead_code)]
    #[inline]
    pub async fn channel_ref(
        &self, system: &ActorSystem,
    ) -> Result<EventSubscriptionChannelRef<T>, ActorRefErr> {
        system
            .get_tracked_actor(self.channel_actor_id())
            .await
            .ok_or_else(|| ActorRefErr::NotFound(self.channel_actor_id()))
    }
}

impl<T: EventCommandTopic> fmt::Debug for EventSubscriptionProcessor<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let topic_type = std::any::type_name::<T>();
        f.debug_struct("EventSubscriptionProcessor")
            .field("topic", &topic_type)
            .field("channel_ref", &self.channel_ref)
            .finish()
    }
}

impl<T: EventCommandTopic> ProcessEntry for EventSubscriptionProcessor<T> {
    type Projection = ();

    fn apply_entry_to_projection(
        &self, _: &Self::Projection, entry: JournalEntry, ctx: &ProcessorContext,
    ) -> ProcessResult<Self::Projection, ProjectionError> {
        // let source_id = ctx.persistence_id().into_actor_id();
        let payload_type = entry.payload_type.clone();

        let envelope = match Self::from_bytes::<T::Event>(entry) {
            Ok(event) => EventEnvelope::new(ctx.persistence_id(), event),
            Err(error) => return ProcessResult::Err(error.into()),
        };

        match self.channel_ref.notify(envelope.clone()) {
            Ok(()) => {
                debug!(
                    ?envelope,
                    "submitted received event envelope, {payload_type}, into subscription channel"
                );
                ProcessResult::Unchanged
            },
            Err(error) => ProcessResult::Err(ProjectionError::Processor(
                coerce_cqrs::projection::processor::ProcessorError::Other(error.into()),
            )),
        }
    }
}
