use coerce::actor::message::Message;
use coerce::persistent::storage::JournalEntry;
use coerce::persistent::PersistentActor;
use coerce_cqrs::projection::processor::{
    ProcessEntry, ProcessResult, Processor, ProcessorContext, ProcessorEngine, ProcessorError,
    ProcessorSourceRef, Ready, RegularInterval,
};
use coerce_cqrs::projection::{PersistenceId, ProjectionError, ProjectionStorage};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Copy, Clone)]
struct TracingApplicator<E: Message + Debug> {
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

    fn apply_entry_to_projection(
        &self, persistence_id: &PersistenceId, projection: &Self::Projection, entry: JournalEntry,
        ctx: &ProcessorContext,
    ) -> ProcessResult<Self::Projection, ProjectionError> {
        match Self::from_bytes::<E>(entry) {
            Ok(event) => info!(
                "EVENT_TRACE: {projection}[{persistence_id}]-{payload_type}#{seq}: {event:?}",
                projection = ctx.projection_name,
                payload_type = entry.payload_type,
                seq = entry.sequence,
            ),
            Err(error) => {
                let type_name = std::any::type_name::<E>();
                error!(
                    "EVENT_TRACE: {projection}[{persistence_id}] failed to convert {type_name} event from bytes: {error:?}",
                    projection = ctx.projection_name,
                )
            },
        }

        ProcessResult::Unchanged
    }
}

pub fn make_tracing_processor_for<A, E, S>(
    label: impl Into<String>, source: ProcessorSourceRef, projection_source: Arc<S>,
) -> Result<ProcessorEngine<Ready<S, TracingApplicator<E>, RegularInterval>>, ProcessorError>
where
    A: PersistentActor,
    E: Message + Debug,
    S: ProjectionStorage,
{
    Processor::builder_for::<A, _, _, _>(label)
        .with_entry_handler(TracingApplicator::default())
        .with_source(source)
        .with_projection_source(projection_source)
        .with_interval_calculator(RegularInterval::of_duration(Duration::from_secs(60)))
        .finish()
}
