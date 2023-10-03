use crate::model::update::{UpdateLocationServicesRef, UpdateLocationsError, UpdateLocationsEvent};
use crate::model::zone::LocationZoneError;
use crate::model::{update, LocationZoneCode, WeatherAlert};
use crate::model::{zone, UpdateLocations};
use crate::services::noaa::AlertApi;
use coerce::actor::system::ActorSystem;
use coerce::actor::{ActorId, ToActorId};
use coerce::persistent::storage::JournalEntry;
use coerce_cqrs::projection::processor::{
    EntryPayloadTypes, ProcessEntry, ProcessResult, ProcessorContext,
};
use coerce_cqrs::projection::ProjectionError;
use coerce_cqrs::Aggregate;
use std::collections::{HashMap, HashSet};
use std::fmt;
use tracing::Instrument;

pub const UPDATE_LOCATIONS_CONTROLLER: &str = "update_locations_controller";

#[derive(Clone)]
pub struct UpdateLocationsController {
    known_entry_type: EntryPayloadTypes,
    system: ActorSystem,
    services: UpdateLocationServicesRef,
}

impl UpdateLocationsController {
    pub fn new(system: ActorSystem, services: UpdateLocationServicesRef) -> Self {
        let known_entry_type =
            EntryPayloadTypes::single(UpdateLocations::journal_message_type_identifier::<
                UpdateLocationsEvent,
            >());
        Self { known_entry_type, system, services }
    }
}

impl fmt::Debug for UpdateLocationsController {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpdateLocationsController")
            .field("system", &self.system.system_id())
            .field("services", &self.services)
            .finish()
    }
}

impl ProcessEntry for UpdateLocationsController {
    type Projection = ();

    fn known_entry_types(&self) -> &EntryPayloadTypes {
        &self.known_entry_type
    }

    #[instrument(level = "debug", skip(self, _projection, entry, ctx))]
    fn apply_entry_to_projection(
        &self, _projection: &Self::Projection, entry: JournalEntry, ctx: &ProcessorContext,
    ) -> ProcessResult<Self::Projection, ProjectionError> {
        let payload_type = entry.payload_type.clone();

        let event = match Self::from_bytes::<UpdateLocationsEvent>(entry) {
            Ok(evt) => evt,
            Err(error) => {
                info!(
                    ?error,
                    "{payload_type} is not an UpdateLocationsEvent -- skipping"
                );
                return ProcessResult::Unchanged;
            },
        };

        if let UpdateLocationsEvent::Started(zones) = event {
            self.do_spawn_update_observations(&zones);
            self.do_spawn_update_forecasts(&zones);
            let saga_id = ctx.persistence_id().to_actor_id();
            self.do_spawn_update_zone_alerts(saga_id, zones);
        }

        ProcessResult::Unchanged
    }
}

type ZoneUpdateFailures = HashMap<LocationZoneCode, LocationZoneError>;

impl UpdateLocationsController {
    fn do_spawn_update_observations(&self, zones: &[LocationZoneCode]) {
        #[allow(clippy::unnecessary_to_owned)]
        for z in zones.iter().cloned() {
            let system = self.system.clone();
            let zone = z.clone();
            tokio::spawn(
                async move { zone::notify_observe(&z, &system).await }
                    .instrument(debug_span!("observe location zone", %zone)),
            );
        }
    }

    fn do_spawn_update_forecasts(&self, zones: &[LocationZoneCode]) {
        #[allow(clippy::unnecessary_to_owned)]
        for z in zones.iter().cloned() {
            let system = self.system.clone();
            let zone = z.clone();
            tokio::spawn(
                async move { zone::notify_forecast(&z, &system).await }
                    .instrument(debug_span!("forecast location zone", %zone)),
            );
        }
    }

    /// Spawn zone weather alert update.
    /// Note: The signature differs from other `do_*` methods to minimize cloning,
    /// so should be called last or changed to be consistent.
    fn do_spawn_update_zone_alerts(&self, saga_id: ActorId, zones: Vec<LocationZoneCode>) {
        let id = saga_id.clone();
        let zs = zones.clone();
        let services = self.services.clone();
        let system = self.system.clone();

        tokio::spawn(
            async move {
                if let Err(error) = do_update_zone_alerts(id, zs, services, system).await {
                    warn!(
                        ?error,
                        "failure during update location weather alerts -- ignoring"
                    );
                }
            }
            .instrument(debug_span!("update location weather alerts", %saga_id, ?zones)),
        );
    }
}

#[instrument(level = "debug", skip(system))]
async fn do_update_zone_alerts(
    saga_id: ActorId, zones: Vec<LocationZoneCode>, services: UpdateLocationServicesRef,
    system: ActorSystem,
) -> Result<(), UpdateLocationsError> {
    let update_scope: HashSet<_> = zones.into_iter().collect();
    let mut alerted_zones = HashSet::with_capacity(update_scope.len());

    // -- zones with alerts
    let alerts = services.active_alerts().await?;
    let nr_alerts = alerts.len();
    let mut update_failures = HashMap::new();

    for alert in alerts {
        let affected: Vec<_> = alert
            .affected_zones
            .clone()
            .into_iter()
            .filter(|z| update_scope.contains(z))
            .collect();

        let (alerted, failures) = do_alert_affected_zones(affected, alert, &system).await;
        alerted_zones.extend(alerted);
        update_failures.extend(failures);
    }

    // -- unaffected zones
    let unaffected_zones: Vec<_> = update_scope.difference(&alerted_zones).cloned().collect();
    info!(?alerted_zones, ?unaffected_zones, %nr_alerts, "DMR: finish alerting with unaffected notes...");
    let unaffected_failures = do_update_unaffected_zones(unaffected_zones, &system).await;
    update_failures.extend(unaffected_failures);

    // -- note update failures
    do_note_alert_update_failures(saga_id.clone(), update_failures, &system).await?;

    // -- note alerts updated as far as they will be - some zones may not emit delta events
    update::note_alerts_updated(saga_id, &system).await?;

    Ok(())
}

#[instrument(level = "trace", skip(system))]
async fn do_alert_affected_zones(
    affected: Vec<LocationZoneCode>, alert: WeatherAlert, system: &ActorSystem,
) -> (Vec<LocationZoneCode>, ZoneUpdateFailures) {
    let mut alerted = vec![];
    let mut failures = ZoneUpdateFailures::new();

    for zone in affected {
        alerted.push(zone.clone());
        if let Err(error) = zone::notify_update_alert(&zone, Some(alert.clone()), system).await {
            failures.insert(zone, error);
        }
    }

    (alerted, failures)
}

#[instrument(level = "trace", skip(system))]
async fn do_update_unaffected_zones(
    unaffected: Vec<LocationZoneCode>, system: &ActorSystem,
) -> ZoneUpdateFailures {
    let mut failures = ZoneUpdateFailures::new();

    for zone in unaffected {
        if let Err(error) = zone::notify_update_alert(&zone, None, system).await {
            failures.insert(zone, error);
        }
    }

    failures
}

#[instrument(level = "debug", skip(system))]
async fn do_note_alert_update_failures(
    saga_id: ActorId, zone_failures: ZoneUpdateFailures, system: &ActorSystem,
) -> Result<(), UpdateLocationsError> {
    let mut errors = vec![];
    for (zone, failure) in zone_failures {
        if let Err(error) =
            update::note_zone_update_failure(saga_id.clone(), zone, failure, system).await
        {
            errors.push(error);
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        warn!(
            ?errors,
            "failed to note {} location failures in `UpdateLocations` saga({saga_id})",
            errors.len()
        );
        Err(errors.pop().unwrap())
    }
}
