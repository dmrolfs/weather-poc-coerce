pub use protocol::EventSubscriptionCommand;

use crate::connect::EventEnvelope;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::{Actor, ActorId, LocalActorRef};
use coerce_cqrs::Aggregate;
use inner::{MultiIndexSubscriptionMap, Subscription};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::{self, Debug};
use tagid::{Entity, Label};

mod protocol {
    use coerce::actor::ActorId;
    use std::collections::HashSet;

    #[derive(Debug, PartialEq, JsonMessage, Serialize, Deserialize)]
    #[result("()")]
    pub enum EventSubscriptionCommand {
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

pub trait EventCommandTopic: Label + Send + Sync + 'static {
    type Source: Aggregate;
    type Subscriber: Actor + Handler<Self::Command>;
    type Event: Message + Debug + Serialize + DeserializeOwned;
    type Command: Message + Debug + Clone;

    fn journal_message_type_indicator() -> &'static str {
        static INDICATOR: once_cell::sync::OnceCell<String> = once_cell::sync::OnceCell::new();
        INDICATOR.get_or_init(Self::Source::journal_message_type_identifier::<Self::Event>)
    }

    fn commands_from_event(
        &self, event_envelope: &EventEnvelope<Self::Event>,
    ) -> Vec<Self::Command>;
}

pub type EventSubscriptionChannelRef<T> = LocalActorRef<EventSubscriptionChannel<T>>;

pub struct EventSubscriptionChannel<T> {
    subscriptions: MultiIndexSubscriptionMap,
    topic: T,
}

impl<T> Debug for EventSubscriptionChannel<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let topic_name = tynm::type_name::<T>();
        f.debug_struct("EventSubscriptionChannel")
            .field("topic", &topic_name)
            .field("subscriptions", &self.subscriptions)
            .finish()
    }
}

impl<T: Label> Label for EventSubscriptionChannel<T> {
    type Labeler = tagid::MakeLabeling<Self>;

    fn labeler() -> Self::Labeler {
        tagid::MakeLabeling::<Self>::default()
    }
}

impl<T: Label> Entity for EventSubscriptionChannel<T> {
    type IdGen = tagid::CuidGenerator;
}

impl<T: EventCommandTopic> EventSubscriptionChannel<T> {
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
impl<T: Send + Sync + 'static> Actor for EventSubscriptionChannel<T> {}

#[async_trait]
impl<T: EventCommandTopic> Handler<EventSubscriptionCommand> for EventSubscriptionChannel<T> {
    #[instrument(level = "debug", skip(self, _ctx))]
    async fn handle(
        &mut self, command: EventSubscriptionCommand, _ctx: &mut ActorContext,
    ) -> <EventSubscriptionCommand as Message>::Result {
        match command {
            EventSubscriptionCommand::SubscribeToPublisher { subscriber_id, publisher_id } => {
                self.add_subscriber(subscriber_id, [publisher_id])
            },

            EventSubscriptionCommand::SubscribeToPublishers { subscriber_id, publisher_ids } => {
                self.add_subscriber(subscriber_id, publisher_ids)
            },

            EventSubscriptionCommand::Unsubscribe { subscriber_id } => {
                self.remove_subscriber(&subscriber_id)
            },
        }
    }
}

#[async_trait]
impl<T: EventCommandTopic> Handler<EventEnvelope<T::Event>> for EventSubscriptionChannel<T> {
    #[instrument(level = "debug", skip(self, ctx))]
    async fn handle(
        &mut self, envelope: EventEnvelope<T::Event>, ctx: &mut ActorContext,
    ) -> <EventEnvelope<T::Event> as Message>::Result {
        let commands = self.topic.commands_from_event(&envelope);
        let publisher_id = envelope.source_id();
        let matched_subscriptions = self.subscriptions.get_by_publisher_id(publisher_id);
        debug!(
            ?matched_subscriptions,
            "[{subscription_name}]: {header}event subscriptions matched to publisher: {publisher_id}",
            subscription_name=tynm::type_name::<T>(),
            header=if matched_subscriptions.is_empty() { "no " } else { "" }
        );

        for subscription in matched_subscriptions {
            self.notify_subscriber_for_event(subscription, &commands, ctx).await;
        }
    }
}

mod inner {
    use coerce::actor::ActorId;
    use multi_index_map::MultiIndexMap;

    #[derive(Debug, Clone, MultiIndexMap)]
    pub struct Subscription {
        #[multi_index(hashed_non_unique)]
        pub subscriber_id: ActorId,

        #[multi_index(hashed_non_unique)]
        pub publisher_id: ActorId,
    }

    impl Subscription {
        pub fn new(subscriber_id: &ActorId, publisher_id: ActorId) -> Self {
            Self { subscriber_id: subscriber_id.clone(), publisher_id }
        }
    }
}
