pub use protocol::EventChannelCommand;

use crate::connect::EventEnvelope;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::{Actor, ActorId, LocalActorRef};
use inner::{MultiIndexSubscriptionMap, Subscription};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;

mod protocol {
    use coerce::actor::ActorId;
    use std::collections::HashSet;

    #[derive(Debug, PartialEq, JsonMessage, Serialize, Deserialize)]
    #[result("()")]
    pub enum EventChannelCommand {
        SubscribeToPublisher {
            subscriber_id: ActorId,
            publisher_id: ActorId,
        },
        SubscribeToPublishers {
            subscriber_id: ActorId,
            publisher_ids: HashSet<ActorId>,
        },
        Unsubscribe {
            subscriber_id: ActorId,
        },
    }
}

pub trait EventBroadcastTopic: Send + Sync + 'static {
    type Subscriber: Actor + Handler<Self::Command>;
    type Event: Message + Debug + Serialize + DeserializeOwned;
    type Command: Message + Debug + Clone;

    fn commands_from_event(
        &self, event_envelope: &EventEnvelope<Self::Event>,
    ) -> Vec<Self::Command>;
}

pub struct EventBroadcastChannel<T: EventBroadcastTopic> {
    subscriptions: MultiIndexSubscriptionMap,
    topic: T,
}

impl<T: EventBroadcastTopic> EventBroadcastChannel<T> {
    pub fn new(topic: T) -> Self {
        Self {
            subscriptions: MultiIndexSubscriptionMap::default(),
            topic,
        }
    }

    fn add_subscriber(
        &mut self, subscriber_id: ActorId, publisher_ids: impl IntoIterator<Item = ActorId>,
    ) {
        for pid in publisher_ids {
            self.subscriptions.insert(Subscription::new(&subscriber_id, pid));
        }
    }

    fn remove_subscriber(&mut self, subscriber_id: &ActorId) {
        let removed = self.subscriptions.remove_by_subscriber_id(subscriber_id);
        let nr_subscriptions = removed.len();
        let publishers: Vec<_> = removed.into_iter().map(|sub| sub.publisher_id).collect();
        debug!(
            ?publishers,
            "Unsubscribed actor, {subscriber_id}, from {nr_subscriptions} event subscriptions."
        );
    }

    async fn subscriber_actor(
        id: ActorId, ctx: &ActorContext,
    ) -> Option<LocalActorRef<T::Subscriber>> {
        ctx.system().get_tracked_actor(id).await
    }

    #[instrument(level = "debug", skip(self, ctx))]
    async fn notify_subscriber_for_event(
        &self, subscription: &Subscription, commands: &[T::Command], ctx: &ActorContext,
    ) {
        let Some(target) = Self::subscriber_actor(subscription.subscriber_id.clone(), ctx).await else {
            error!("failed to get a handle on subscriber: {} -- skipping", subscription.subscriber_id);
            return;
        };

        for command in commands {
            if let Err(error) = target.notify(command.clone()) {
                error!(
                    ?error,
                    ?command,
                    "failed to send command to subscriber, {} -- skipping subscriber",
                    subscription.subscriber_id
                );
                return;
            }
        }
    }
}

#[async_trait]
impl<T: EventBroadcastTopic> Actor for EventBroadcastChannel<T> {}

#[async_trait]
impl<T: EventBroadcastTopic> Handler<EventChannelCommand> for EventBroadcastChannel<T> {
    #[instrument(level = "debug", skip(self, _ctx))]
    async fn handle(
        &mut self, command: EventChannelCommand, _ctx: &mut ActorContext,
    ) -> <EventChannelCommand as Message>::Result {
        match command {
            EventChannelCommand::SubscribeToPublisher { subscriber_id, publisher_id } => {
                self.add_subscriber(subscriber_id, [publisher_id])
            },

            EventChannelCommand::SubscribeToPublishers { subscriber_id, publisher_ids } => {
                self.add_subscriber(subscriber_id, publisher_ids)
            },

            EventChannelCommand::Unsubscribe { subscriber_id } => {
                self.remove_subscriber(&subscriber_id)
            },
        }
    }
}

#[async_trait]
impl<T: EventBroadcastTopic> Handler<EventEnvelope<T::Event>> for EventBroadcastChannel<T> {
    #[instrument(level = "debug", skip(self, ctx))]
    async fn handle(
        &mut self, envelope: EventEnvelope<T::Event>, ctx: &mut ActorContext,
    ) -> <EventEnvelope<T::Event> as Message>::Result {
        let commands = self.topic.commands_from_event(&envelope);
        for subscription in self.subscriptions.get_by_publisher_id(envelope.source_id()) {
            self.notify_subscriber_for_event(subscription, &commands, ctx).await;
        }
    }
}

mod inner {
    use coerce::actor::ActorId;
    use multi_index_map::MultiIndexMap;

    #[derive(Debug, MultiIndexMap)]
    pub struct Subscription {
        #[multi_index(hashed_non_unique)]
        pub subscriber_id: ActorId,

        #[multi_index(hashed_non_unique)]
        pub publisher_id: ActorId,
    }

    impl Subscription {
        pub const fn new(subscriber_id: &ActorId, publisher_id: ActorId) -> Self {
            Self { subscriber_id: subscriber_id.clone(), publisher_id }
        }
    }
}
