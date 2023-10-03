use crate::model::update::location_status::{LocationStatus, MultiIndexLocationStatusMap};
use crate::model::update::{UpdateLocationsCommand, UpdateLocationsError, UpdateLocationsEvent};
use crate::model::LocationZoneCode;
use coerce_cqrs::{AggregateState, CommandResult};
use either::{Either, Left, Right};
use enumflags2::{bitflags, BitFlags};
use once_cell::sync::Lazy;
use strum_macros::Display;

#[derive(Debug, Clone, PartialEq, ToSchema, Serialize, Deserialize)]
pub enum UpdateLocationsState {
    Quiescent(QuiescentLocationsUpdate),
    Active(ActiveLocationsUpdate),
    Finished(FinishedLocationsUpdate),
}

impl Default for UpdateLocationsState {
    fn default() -> Self {
        Self::Quiescent(QuiescentLocationsUpdate)
    }
}

#[async_trait]
impl AggregateState<UpdateLocationsCommand, UpdateLocationsEvent> for UpdateLocationsState {
    type Error = UpdateLocationsError;
    type State = Self;

    fn handle_command(
        &self, command: &UpdateLocationsCommand,
    ) -> CommandResult<Vec<UpdateLocationsEvent>, Self::Error> {
        match self {
            Self::Quiescent(state) => state.handle_command(command),
            Self::Active(state) => state.handle_command(command),
            Self::Finished(state) => state.handle_command(command),
        }
    }

    fn apply_event(&mut self, event: UpdateLocationsEvent) -> Option<Self::State> {
        match self {
            Self::Quiescent(state) => state.apply_event(event),
            Self::Active(state) => state.apply_event(event),
            Self::Finished(state) => state.apply_event(event),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode, Serialize, Deserialize)]
pub struct QuiescentLocationsUpdate;

impl AggregateState<UpdateLocationsCommand, UpdateLocationsEvent> for QuiescentLocationsUpdate {
    type Error = UpdateLocationsError;
    type State = UpdateLocationsState;

    fn handle_command(
        &self, command: &UpdateLocationsCommand,
    ) -> CommandResult<Vec<UpdateLocationsEvent>, Self::Error> {
        use UpdateLocationsCommand as C;

        match command {
            C::UpdateLocations(zones) if !zones.is_empty() => {
                debug!("DMR: Saga starting to update zones: {zones:?}");
                CommandResult::Ok(vec![UpdateLocationsEvent::Started(zones.clone())])
            },

            C::UpdateLocations(_empty_zones) => CommandResult::Rejected(format!(
                "UpdateLocations asked to start updating empty set of zones: {command:?}"
            )),

            cmd => CommandResult::Rejected(format!(
                "UpdateLocations saga cannot handle command until it starts an update: {:?}",
                cmd
            )),
        }
    }

    fn apply_event(&mut self, event: UpdateLocationsEvent) -> Option<Self::State> {
        use UpdateLocationsEvent as E;

        match event {
            E::Started(zones) => {
                let mut location_statuses = MultiIndexLocationStatusMap::with_capacity(zones.len());
                for zone in zones {
                    let ls = LocationStatus { zone, status: *DEFAULT_LOCATION_UPDATE_STATUS };
                    location_statuses.insert(ls);
                }

                Some(UpdateLocationsState::Active(ActiveLocationsUpdate {
                    location_statuses,
                    alerts_reviewed: false,
                }))
            },

            event => {
                warn!(
                    ?event,
                    "unrecognized update locations saga event while quiescent -- ignored"
                );
                None
            },
        }
    }
}

#[bitflags]
#[repr(u8)]
#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, ToSchema, Serialize, Deserialize)]
pub enum LocationUpdateStep {
    Observation = 0b0001,
    Forecast = 0b0010,
    Alert = 0b0100,
}

pub type LocationUpdateSteps = BitFlags<LocationUpdateStep>;

#[derive(Debug, Display, Copy, Clone, PartialEq, Eq, Hash, ToSchema, Serialize, Deserialize)]
pub enum UpdateCompletionStatus {
    Succeeded,
    Failed,
}

pub static DEFAULT_LOCATION_UPDATE_STATUS: Lazy<LocationUpdateStatus> =
    Lazy::new(|| Left(LocationUpdateSteps::default()));

pub type LocationUpdateStatus = Either<LocationUpdateSteps, UpdateCompletionStatus>;

pub trait LocationUpdateStatusExt {
    fn is_active(&self) -> bool;
    fn is_completed(&self) -> bool;
}

impl LocationUpdateStatusExt for LocationUpdateStatus {
    #[inline]
    fn is_active(&self) -> bool {
        !self.is_completed()
    }

    #[inline]
    fn is_completed(&self) -> bool {
        match self {
            Right(_) => true,
            Left(steps_completed) => {
                steps_completed.contains(LocationUpdateStep::Observation)
                    && steps_completed.contains(LocationUpdateStep::Forecast)
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, ToSchema, Serialize, Deserialize)]
pub struct ActiveLocationsUpdate {
    pub location_statuses: MultiIndexLocationStatusMap,
    pub alerts_reviewed: bool,
}

#[async_trait]
impl AggregateState<UpdateLocationsCommand, UpdateLocationsEvent> for ActiveLocationsUpdate {
    type Error = UpdateLocationsError;
    type State = UpdateLocationsState;

    #[instrument(level = "debug")]
    fn handle_command(
        &self, command: &UpdateLocationsCommand,
    ) -> CommandResult<Vec<UpdateLocationsEvent>, Self::Error> {
        use LocationUpdateStep as Step;
        use UpdateLocationsCommand as C;

        match command {
            C::NoteLocationObservationUpdate(zone) => {
                self.handle_location_update(zone.clone(), Step::Observation)
            },

            C::NoteLocationForecastUpdate(zone) => {
                self.handle_location_update(zone.clone(), Step::Forecast)
            },

            C::NoteLocationAlertStatusUpdate(zone) => {
                self.handle_location_update(zone.clone(), Step::Alert)
            },

            C::NoteLocationsUpdateFailure(zone) => self.handle_location_failure(zone.clone()),

            C::NoteAlertsUpdated => self.handle_alerts_updated(),

            C::UpdateLocations(_) => CommandResult::Rejected(
                "UpdateLocations saga already updating locations".to_string(),
            ),
        }
    }

    #[instrument(level = "debug")]
    fn apply_event(&mut self, event: UpdateLocationsEvent) -> Option<Self::State> {
        use UpdateLocationsEvent as E;

        match event {
            E::LocationUpdated(zone, status) => {
                let mut new_state = self.clone();

                if let Some(previous) = self.location_statuses.get_by_zone(&zone) {
                    info!(
                        "updated location zone {zone} status: {} => {status}",
                        previous.status
                    );
                }

                new_state.location_statuses.modify_by_zone(&zone, |ls| {
                    ls.status = status;
                });

                Some(Self::State::Active(new_state))
            },

            E::AlertsUpdated => {
                let mut new_state = self.clone();
                new_state.alerts_reviewed = true;
                Some(Self::State::Active(new_state))
            },

            E::Completed | E::Failed => Some(Self::State::Finished(FinishedLocationsUpdate)),

            E::Started(_) => {
                warn!(
                    ?event,
                    "unrecognized update locations saga event while active -- ignored"
                );
                None
            },
        }
    }
}

impl ActiveLocationsUpdate {
    fn status_for(&self, zone: &LocationZoneCode) -> LocationUpdateStatus {
        let status = self.location_statuses.get_by_zone(zone).map(|ls| ls.status);
        status.unwrap_or(*DEFAULT_LOCATION_UPDATE_STATUS)
    }

    #[instrument(level = "debug")]
    fn handle_location_update(
        &self, zone: LocationZoneCode, step: LocationUpdateStep,
    ) -> CommandResult<Vec<UpdateLocationsEvent>, UpdateLocationsError> {
        use UpdateLocationsEvent as E;

        let previous = self.status_for(&zone).left();

        debug!(status=?self.location_statuses, "is {zone} only active: {}", self.is_only_active_zone(&zone));

        let events = match (previous, step) {
            (None, _) => vec![],
            (Some(previous), current) if previous.contains(current) => vec![],
            (Some(mut zone_steps), current) => {
                zone_steps.toggle(current);
                if zone_steps.is_all() && self.alerts_reviewed {
                    use UpdateCompletionStatus as Status;
                    let is_only_active_zone = self.is_only_active_zone(&zone);
                    let mut result = vec![E::LocationUpdated(zone, Right(Status::Succeeded))];
                    if is_only_active_zone {
                        result.push(E::Completed)
                    }
                    result
                } else {
                    vec![E::LocationUpdated(zone, Left(zone_steps))]
                }
            },
        };

        CommandResult::Ok(events)
    }

    #[instrument(level = "debug")]
    fn handle_alerts_updated(
        &self,
    ) -> CommandResult<Vec<UpdateLocationsEvent>, UpdateLocationsError> {
        use UpdateLocationsEvent as E;
        let mut events = vec![E::AlertsUpdated];

        let all_zones_done =
            self.location_statuses.iter_by_status().all(|ls| ls.status.is_completed());
        if all_zones_done {
            events.push(E::Completed)
        }

        CommandResult::Ok(events)
    }

    #[instrument(level = "debug")]
    fn handle_location_failure(
        &self, zone: LocationZoneCode,
    ) -> CommandResult<Vec<UpdateLocationsEvent>, UpdateLocationsError> {
        let previous_status = self.status_for(&zone);
        if previous_status.is_active() {
            use UpdateCompletionStatus as Status;
            use UpdateLocationsEvent as E;
            let is_only_active_zone = self.is_only_active_zone(&zone);
            let mut events = vec![E::LocationUpdated(zone, Right(Status::Failed))];
            if is_only_active_zone {
                events.push(E::Failed)
            }
            CommandResult::Ok(events)
        } else {
            CommandResult::Ok(vec![])
        }
    }

    #[inline]
    fn is_only_active_zone(&self, zone: &LocationZoneCode) -> bool {
        self.location_statuses
            .iter_by_status()
            .any(|ls| ls.status.is_active() && zone != &ls.zone)
    }
}

#[derive(Debug, Clone, PartialEq, bitcode::Encode, bitcode::Decode, Serialize, Deserialize)]
pub struct FinishedLocationsUpdate;

#[async_trait]
impl AggregateState<UpdateLocationsCommand, UpdateLocationsEvent> for FinishedLocationsUpdate {
    type Error = UpdateLocationsError;
    type State = UpdateLocationsState;

    #[instrument(level = "debug", skip(self))]
    fn handle_command(
        &self, command: &UpdateLocationsCommand,
    ) -> CommandResult<Vec<UpdateLocationsEvent>, Self::Error> {
        CommandResult::Rejected(format!(
            "Finished UpdateLocations saga does not handle further commands: {command:?}"
        ))
    }

    #[instrument(level = "debug", skip(self))]
    fn apply_event(&mut self, event: UpdateLocationsEvent) -> Option<Self::State> {
        warn!(
            ?event,
            "unrecognized update locations saga event while finished -- ignored"
        );
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::update::state::{LocationUpdateSteps, DEFAULT_LOCATION_UPDATE_STATUS};
    use claim::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_location_status_map() -> anyhow::Result<()> {
        let mut locstats = MultiIndexLocationStatusMap::with_capacity(3);
        let zones: Vec<LocationZoneCode> = vec!["WAZ558".into(), "ILZ045".into(), "KYZ069".into()];
        for zone in zones.clone() {
            let ls = LocationStatus { zone, status: *DEFAULT_LOCATION_UPDATE_STATUS };
            locstats.insert(ls);
        }
        assert_eq!(locstats.len(), 3);
        let actual_zones_1: Vec<_> = locstats
            .get_by_status(&DEFAULT_LOCATION_UPDATE_STATUS)
            .into_iter()
            .cloned()
            .map(|ls| ls.zone)
            .collect();
        assert_eq!(actual_zones_1, zones);

        let mut new_status = LocationUpdateSteps::empty();
        new_status.toggle(LocationUpdateStep::Observation);
        locstats.modify_by_zone(&"WAZ558".into(), |ls| {
            ls.status = Either::<_, UpdateCompletionStatus>::Left(new_status.clone());
        });
        assert_eq!(locstats.len(), 3);

        let actual_zones_2: Vec<_> = locstats
            .get_by_status(&DEFAULT_LOCATION_UPDATE_STATUS)
            .into_iter()
            .cloned()
            .map(|ls| ls.zone)
            .collect();
        assert_eq!(actual_zones_2, vec!["ILZ045".into(), "KYZ069".into()]);

        let actual_zones_3: Vec<_> = locstats
            .get_by_status(&Left(new_status))
            .into_iter()
            .cloned()
            .map(|ls| ls.zone)
            .collect();
        assert_eq!(actual_zones_3, vec!["WAZ558".into()]);

        Ok(())
    }
}
