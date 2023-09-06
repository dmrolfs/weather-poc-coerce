pub mod event_broadcast;

use coerce::actor::message::Message;
use coerce::actor::{ActorId, IntoActorId};
use serde::de::{DeserializeOwned, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(JsonMessage)]
#[result("()")]
pub struct EventEnvelope<E>
where
    E: Message + Serialize + DeserializeOwned,
{
    inner: Arc<inner::EventEnvelopeRef<E>>,
    // _marker: PhantomData<E>,
}

impl<E> fmt::Debug for EventEnvelope<E>
where
    E: fmt::Debug + Message + Serialize + DeserializeOwned,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageEnvelope")
            .field("source_id", &self.inner.source_id)
            .field("event", &self.inner.event)
            .field("metadata", &self.inner.metadata)
            .finish()
    }
}

impl<E> Clone for EventEnvelope<E>
where
    E: Message + Serialize + DeserializeOwned,
{
    fn clone(&self) -> Self {
        // Self { inner: self.inner.clone(), _marker: self._marker.clone() }
        Self { inner: self.inner.clone() }
    }
}

impl<E> EventEnvelope<E>
where
    E: Message + Serialize + DeserializeOwned,
{
    pub fn new(source_id: impl IntoActorId, event: E) -> Self {
        Self::new_with_metadata(source_id, event, HashMap::new())
    }

    pub fn new_with_metadata(
        source_id: impl IntoActorId, event: E, metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            inner: Arc::new(inner::EventEnvelopeRef {
                source_id: source_id.into_actor_id(),
                event,
                metadata,
            }),
            // _marker: PhantomData,
        }
    }

    pub fn source_id(&self) -> &ActorId {
        &self.inner.source_id
    }

    pub fn event(&self) -> &E {
        &self.inner.event
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.inner.metadata
    }
}

impl<E> EventEnvelope<E>
where
    E: Clone + Message + Serialize + DeserializeOwned,
{
    pub fn as_parts(&self) -> (ActorId, E, HashMap<String, String>) {
        (
            self.source_id().clone(),
            self.event().clone(),
            self.metadata().clone(),
        )
    }
}

impl<E> Serialize for EventEnvelope<E>
where
    E: Message + Serialize + DeserializeOwned,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("EventEnvelope", 3)?;
        state.serialize_field("source_id", self.source_id())?;
        state.serialize_field("event", self.event())?;
        state.serialize_field("metadata", self.metadata())?;
        state.end()
    }
}

impl<'de, E> Deserialize<'de> for EventEnvelope<E>
where
    E: Message + Serialize + DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            SourceId,
            Event,
            Metadata,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D0>(deserializer: D0) -> Result<Field, D0::Error>
            where
                D0: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("`source_id` or `event` or `metadata`")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "source_id" => Ok(Field::SourceId),
                            "event" => Ok(Field::Event),
                            "metadata" => Ok(Field::Metadata),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct EventEnvelopeVisitor<E0: Message> {
            _marker: PhantomData<fn() -> E0>,
        }

        impl<E0: Message> Default for EventEnvelopeVisitor<E0> {
            fn default() -> Self {
                Self { _marker: PhantomData }
            }
        }

        impl<E0: Message> EventEnvelopeVisitor<E0> {
            fn my_type_name() -> String {
                format!("EventEnvelope<{}>", std::any::type_name::<E0>())
            }
        }

        impl<'de, E0> Visitor<'de> for EventEnvelopeVisitor<E0>
        where
            E0: Message + Serialize + DeserializeOwned,
        {
            type Value = EventEnvelope<E0>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(Self::my_type_name().as_ref())
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Self::Value, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let source_id: ActorId =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let event =
                    seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let metadata = seq.next_element()?.unwrap_or_default();
                Ok(EventEnvelope::new_with_metadata(source_id, event, metadata))
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut source_id = None;
                let mut event = None;
                let mut metadata = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::SourceId => {
                            if source_id.is_some() {
                                return Err(de::Error::duplicate_field("source_id"));
                            }
                            source_id = Some(map.next_value()?);
                        },

                        Field::Event => {
                            if event.is_some() {
                                return Err(de::Error::duplicate_field("event"));
                            }
                            event = Some(map.next_value()?);
                        },

                        Field::Metadata => {
                            if metadata.is_some() {
                                return Err(de::Error::duplicate_field("metadata"));
                            }
                            metadata = Some(map.next_value()?);
                        },
                    }
                }

                let source_id: ActorId =
                    source_id.ok_or_else(|| de::Error::missing_field("source_id"))?;
                let event = event.ok_or_else(|| de::Error::missing_field("event"))?;
                let metadata = metadata.unwrap_or_default();
                Ok(EventEnvelope::new_with_metadata(source_id, event, metadata))
            }
        }

        const FIELDS: &[&str] = &["source_id", "event", "metadata"];
        deserializer.deserialize_struct(
            EventEnvelopeVisitor::<E>::my_type_name().as_ref(),
            FIELDS,
            EventEnvelopeVisitor::default(),
        )
    }
}

pub struct CommandEnvelope<C>
where
    C: Message,
{
    inner: Arc<inner::CommandEnvelopeRef<C>>,
    // _marker: PhantomData<(A, C)>,
}

impl<C> fmt::Debug for CommandEnvelope<C>
where
    C: Message + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageEnvelope")
            .field("target_id", &self.inner.target_id)
            .field("command", &self.inner.command)
            .field("metadata", &self.inner.metadata)
            .finish()
    }
}

impl<C> Clone for CommandEnvelope<C>
where
    C: Message,
{
    fn clone(&self) -> Self {
        // Self { inner: self.inner.clone(), _marker: self._marker.clone(), }
        Self { inner: self.inner.clone() }
    }
}

impl<C> CommandEnvelope<C>
where
    C: Message,
{
    pub fn new(target_id: impl IntoActorId, command: C) -> Self {
        Self::new_with_metadata(target_id, command, HashMap::new())
    }

    pub fn new_with_metadata(
        target_id: impl IntoActorId, command: C, metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            inner: Arc::new(inner::CommandEnvelopeRef {
                target_id: target_id.into_actor_id(),
                command,
                metadata,
            }),
            // _marker: PhantomData,
        }
    }

    pub fn target_id(&self) -> &ActorId {
        &self.inner.target_id
    }

    pub fn command(&self) -> &C {
        &self.inner.command
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.inner.metadata
    }
}

impl<C> CommandEnvelope<C>
where
    C: Message + Clone,
{
    pub fn as_parts(&self) -> (ActorId, C, HashMap<String, String>) {
        (
            self.target_id().clone(),
            self.command().clone(),
            self.metadata().clone(),
        )
    }
}

mod inner {
    use coerce::actor::message::Message;
    use coerce::actor::ActorId;
    use std::collections::HashMap;

    pub struct EventEnvelopeRef<E>
    where
        E: Message,
    {
        pub source_id: ActorId,
        pub event: E,
        pub metadata: HashMap<String, String>,
    }

    pub struct CommandEnvelopeRef<C>
    where
        C: Message,
    {
        pub target_id: ActorId,
        pub command: C,
        pub metadata: HashMap<String, String>,
    }
}
