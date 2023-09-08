use crate::model::update::{UpdateLocationServicesRef, UpdateLocationsEvent};
use crate::model::zone;
use crate::model::zone::LocationZoneError;
use crate::model::{update, LocationZoneCode, WeatherAlert};
use crate::services::noaa::AlertApi;
use coerce::actor::system::ActorSystem;
use coerce::actor::{ActorId, IntoActorId};
use coerce::persistent::storage::JournalEntry;
use coerce_cqrs::projection::processor::{ProcessEntry, ProcessResult, ProcessorContext};
use coerce_cqrs::projection::{PersistenceId, ProjectionError};
use std::collections::{HashMap, HashSet};
use std::fmt;
use tracing::Instrument;

#[derive(Clone)]
pub struct UpdateLocationsController {
    system: ActorSystem,
    services: UpdateLocationServicesRef,
}

impl UpdateLocationsController {
    pub fn new(system: ActorSystem, services: UpdateLocationServicesRef) -> Self {
        Self { system, services }
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

    #[instrument(level = "debug", skip(self, _projection, entry, _ctx))]
    fn apply_entry_to_projection(
        &self, persistence_id: &PersistenceId, _projection: &Self::Projection, entry: JournalEntry,
        _ctx: &ProcessorContext,
    ) -> ProcessResult<Self::Projection, ProjectionError> {
        let payload_type = entry.payload_type.clone();

        let event = match Self::from_bytes::<UpdateLocationsEvent>(entry) {
            Ok(evt) => evt,
            Err(error) => {
                info!(?error, "{payload_type} is not an UpdateLocationsEvent -- skipping");
                return ProcessResult::Unchanged;
            },
        };

        if let UpdateLocationsEvent::Started(zones) = event {
            let saga_id = persistence_id.into_actor_id();
            self.do_spawn_update_observations(&zones);
            self.do_spawn_update_forecasts(&zones);
            self.do_spawn_update_zone_alerts(&saga_id, &zones);
        }

        ProcessResult::Unchanged
    }
}

type ZoneUpdateFailures = HashMap<LocationZoneCode, LocationZoneError>;

impl UpdateLocationsController {
    fn do_spawn_update_observations(&self, zones: &[LocationZoneCode]) {
        for z in zones {
            tokio::spawn(
                async {
                    zone::notify_observe(z, &self.system).await
                }
                    .instrument(debug_span!("observe location zone", zone=%z))
            );
        }
    }

    fn do_spawn_update_forecasts(&self, zones: &[LocationZoneCode]) {
        for z in zones {
            tokio::spawn(
                async {
                    zone::notify_forecast(z, &self.system).await
                }
                    .instrument(debug_span!("forecast location zone", zone=%z))
            );
        }
    }

    fn do_spawn_update_zone_alerts(&self, saga_id: &ActorId, zones: &[LocationZoneCode]) {
        tokio::spawn(
            async {
                self.do_update_zone_alerts(saga_id, zones)
            }
                .instrument(debug_span!("update location weather alerts", %saga_id, ?zones))
        );
    }

    async fn do_update_zone_alerts(&self, saga_id: &ActorId, zones: &[LocationZoneCode]) {
        let update_zones: HashSet<_> = zones.iter().cloned().collect();
        let mut alerted_zones = HashSet::with_capacity(update_zones.len());

        // -- zones with alerts
        let alerts = self.do_get_alerts().await;
        let nr_alerts = alerts.len();
        let mut zone_update_failures = HashMap::new();
        for alert in alerts {
            let saga_affected_zones: Vec<_> = alert
                .affected_zones
                .clone()
                .into_iter()
                .filter(|z| update_zones.contains(z))
                .collect();

            let (affected_zones, failures) =
                self.do_alert_affected_zones(alert, saga_affected_zones).await;
            alerted_zones.extend(affected_zones);
            zone_update_failures.extend(failures);
        }

        // -- unaffected zones
        let unaffected_zones: Vec<_> = update_zones.difference(&alerted_zones).cloned().collect();
        info!(?alerted_zones, ?unaffected_zones, %nr_alerts, "DMR: finishing alerting with unaffected notes...");
        let unaffected_failures = self.do_update_unaffected_zones(unaffected_zones).await;
        zone_update_failures.extend(unaffected_failures);

        // -- note update failures
        self.do_note_alert_update_failures(saga_id.clone(), zone_update_failures)
            .await;
    }

    #[instrument(level = "trace", skip(self,))]
    async fn do_alert_affected_zones(
        &self, alert: WeatherAlert, affected_zones: Vec<LocationZoneCode>,
    ) -> (Vec<LocationZoneCode>, ZoneUpdateFailures) {
        let mut alerted_zones = vec![];
        let mut failures = ZoneUpdateFailures::new();

        for zone in affected_zones {
            alerted_zones.push(zone.clone());
            if let Err(error) =
                zone::notify_update_alert(&zone, Some(alert.clone()), &self.system).await
            {
                failures.insert(zone, error);
            }
        }

        (alerted_zones, failures)
    }

    #[instrument(level = "trace", skip(self))]
    async fn do_update_unaffected_zones(
        &self, unaffected: Vec<LocationZoneCode>,
    ) -> ZoneUpdateFailures {
        let mut failures = ZoneUpdateFailures::new();

        for zone in unaffected {
            if let Err(error) = zone::notify_update_alert(&zone, None, &self.system).await {
                failures.insert(zone, error);
            }
        }

        failures
    }

    #[instrument(level = "trace", skip(self))]
    async fn do_note_alert_update_failures(&self, saga_id: ActorId, failures: ZoneUpdateFailures) {
        for (zone, failure) in failures {
            if let Err(error) =
                update::note_zone_update_failure(saga_id.clone(), zone, failure, &self.system).await
            {
                warn!(
                    ?error,
                    "failed to note location update failure in `UpdateLocations` saga -- ignoring"
                );
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    async fn do_get_alerts(&self) -> Vec<WeatherAlert> {
        match self.services.active_alerts().await {
            Ok(alerts) => alerts,
            Err(error) => {
                warn!(?error, "failed to pull NOAA weather alerts -- skipping");
                vec![]
            },
        }
    }
}
