use coerce::actor::message::Message;
use coerce::persistent::storage::JournalEntry;
use coerce::persistent::PersistentActor;
use coerce_cqrs::projection::processor::{
    EntryPayloadTypes, ProcessEntry, ProcessResult, Processor, ProcessorContext, ProcessorEngine,
    ProcessorError, ProcessorSourceRef, Ready, RegularInterval,
};
use coerce_cqrs::projection::{PersistenceId, ProjectionError, ProjectionStorageRef};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Duration;

#[derive(Debug, Copy, Clone)]
pub struct TracingApplicator<E: Message + Debug> {
    _marker: PhantomData<E>,
}

impl<E: Message + Debug> Default for TracingApplicator<E> {
    fn default() -> Self {
        Self { _marker: PhantomData }
    }
}

#[async_trait]
impl<E: Message + Debug> ProcessEntry for TracingApplicator<E> {
    type Projection = ();

    fn known_entry_types(&self) -> &EntryPayloadTypes {
        &EntryPayloadTypes::All
    }

    fn apply_entry_to_projection(
        &self, projection: &Self::Projection, entry: JournalEntry, ctx: &ProcessorContext,
    ) -> ProcessResult<Self::Projection, ProjectionError> {
        let seq = entry.sequence;
        let payload_type = entry.payload_type.clone();
        match Self::from_bytes::<E>(entry) {
            Ok(event) => info!(
                ?projection,
                "EVENT_TRACE: {name}[{persistence_id}]-{payload_type}#{seq}: {event:?}",
                name = ctx.projection_name,
                persistence_id = ctx.persistence_id(),
            ),
            Err(error) => {
                let type_name = tynm::type_name::<E>();
                error!(
                    "EVENT_TRACE: {projection}[{persistence_id}] failed to convert {type_name} event from bytes: {error:?}",
                    persistence_id = ctx.persistence_id(),
                    projection = ctx.projection_name,
                )
            },
        }

        ProcessResult::Unchanged
    }
}

pub type TracingProcessorEngine<E> =
    ProcessorEngine<Ready<PersistenceId, (), TracingApplicator<E>, RegularInterval>>;

#[allow(dead_code)]
pub fn make_tracing_processor_for<A, E>(
    label: impl Into<String>, source: ProcessorSourceRef,
    projection_source: ProjectionStorageRef<PersistenceId, ()>,
) -> Result<TracingProcessorEngine<E>, ProcessorError>
where
    A: PersistentActor,
    E: Message + Debug,
{
    Processor::builder_for::<A, _, _, _, _>(label)
        .with_entry_handler(TracingApplicator::default())
        .with_source(source)
        .with_projection_storage(projection_source)
        .with_interval_calculator(RegularInterval::of_duration(Duration::from_secs(60)))
        .finish()
}
