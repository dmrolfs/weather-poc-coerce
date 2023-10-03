use super::state::UpdateLocationsState;
use crate::model::update::UpdateLocationsEvent;
use coerce_cqrs::postgres::{BinaryProjection, PostgresStorageConfig, TableName};
use coerce_cqrs::projection::processor::ProcessResult;
use coerce_cqrs::projection::{PersistenceId, ProjectionError, ProjectionStorageRef};
use coerce_cqrs::AggregateState;
use once_cell::sync::Lazy;

pub const UPDATE_LOCATIONS_HISTORY_VIEW: &str = "update_locations_history";
pub static UPDATE_LOCATIONS_HISTORY_TABLE: Lazy<TableName> =
    Lazy::new(|| TableName::new(UPDATE_LOCATIONS_HISTORY_VIEW).unwrap());
pub static UPDATE_LOCATIONS_OFFSET_TABLE: Lazy<TableName> =
    Lazy::new(PostgresStorageConfig::default_projection_offsets_table);

pub type UpdateLocationsHistoryProjection =
    ProjectionStorageRef<PersistenceId, UpdateLocationsHistory>;

#[derive(Debug, Default, Clone, PartialEq, ToSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateLocationsHistory {
    pub state: UpdateLocationsState,
    pub history: Vec<UpdateLocationsEvent>,
}

impl BinaryProjection for UpdateLocationsHistory {
    type BinaryCodecError = bitcode::Error;

    fn as_bytes(&self) -> Result<Vec<u8>, Self::BinaryCodecError> {
        bitcode::serialize(self)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::BinaryCodecError> {
        bitcode::deserialize(bytes)
    }
}

impl UpdateLocationsHistory {
    #[instrument(level = "debug")]
    pub fn apply_event(
        _: &PersistenceId, history: &Self, event: UpdateLocationsEvent,
    ) -> ProcessResult<Self, ProjectionError> {
        let mut updated_history = history.clone();
        updated_history.history.push(event.clone());

        if let Some(new_state) = updated_history.state.clone().apply_event(event) {
            updated_history.state = new_state;
        }

        ProcessResult::Changed(updated_history)
    }
}
