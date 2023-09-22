use crate::model::registrar::protocol::RegistrarEvent;
use crate::model::LocationZoneCode;
use coerce_cqrs::postgres::{PostgresProjectionStorage, TableName};
use coerce_cqrs::projection::processor::ProcessResult;
use coerce_cqrs::projection::{PersistenceId, ProjectionError};
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::sync::Arc;

pub const MONITORED_ZONES_VIEW: &str = "monitored_zones";
pub static MONITORED_ZONES_TABLE: Lazy<TableName> =
    Lazy::new(|| TableName::new(MONITORED_ZONES_VIEW).unwrap());
pub static REGISTRAR_OFFSET_TABLE: Lazy<TableName> =
    Lazy::new(|| TableName::new("projection_offset").unwrap());

pub type MonitoredZonesProjection = Arc<PostgresProjectionStorage<MonitoredZonesView>>;

#[derive(Debug, Default, Clone, PartialEq, ToSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MonitoredZonesView {
    pub zones: HashSet<LocationZoneCode>,
}

impl MonitoredZonesView {
    #[instrument(level = "debug")]
    pub fn apply_event(
        _pid: &PersistenceId, monitored: &MonitoredZonesView, event: RegistrarEvent,
    ) -> ProcessResult<MonitoredZonesView, ProjectionError> {
        let mut updated_zones = monitored.clone();

        use super::protocol::RegistrarEvent as E;
        match event {
            E::ForecastZoneAdded(zone) => {
                updated_zones.zones.insert(zone);
            },
            E::ForecastZoneForgotten(zone) => {
                updated_zones.zones.remove(&zone);
            },
            E::AllForecastZonesForgotten => {
                updated_zones.zones.clear();
            },
        }

        ProcessResult::Changed(updated_zones)
    }
}
