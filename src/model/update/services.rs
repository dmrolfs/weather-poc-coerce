use crate::connect::{EventSubscriptionChannelRef, EventSubscriptionCommand};
use crate::model::update::{LocationZoneBroadcastTopic, UpdateLocationsError, UpdateLocationsId};
use crate::model::{LocationZoneCode, WeatherAlert};
use crate::services::noaa::{AlertApi, NoaaWeatherError, NoaaWeatherServices};
use coerce::actor::system::ActorSystem;
use coerce::actor::{ActorId, ActorRefErr, IntoActorId};
use once_cell::sync::OnceCell;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

pub type UpdateLocationServicesRef = Arc<UpdateLocationServices>;

static SERVICES: OnceCell<UpdateLocationServicesRef> = OnceCell::new();

/// Initializes the `UpdateLocationServices` used by `UpdateLocations` actors. This may be
/// initialized once, and will return the supplied value in an Err (i.e., `Err(services)`) on subsequent calls.
pub fn initialize_services(
    services: UpdateLocationServicesRef,
) -> Result<(), UpdateLocationServicesRef> {
    SERVICES.set(services)
}

pub fn services() -> UpdateLocationServicesRef {
    SERVICES.get().expect("UpdateLocationServices are not initialized").clone()
}

#[derive(Clone)]
pub struct UpdateLocationServices {
    noaa: NoaaWeatherServices,
    location_subscription_actor_id: ActorId,
    system: ActorSystem,
}

impl fmt::Debug for UpdateLocationServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpdateLocationServices")
            .field("noaa", &self.noaa)
            .field(
                "location_subscription_actor_id",
                &self.location_subscription_actor_id,
            )
            .field("system", &self.system.system_id())
            .finish()
    }
}

impl UpdateLocationServices {
    pub fn new(
        noaa: NoaaWeatherServices, location_subscription_actor_id: ActorId, system: ActorSystem,
    ) -> Self {
        Self { noaa, location_subscription_actor_id, system }
    }

    pub async fn add_subscriber(
        &self, subscriber_id: UpdateLocationsId, zones: &[LocationZoneCode],
    ) -> Result<(), UpdateLocationsError> {
        let subscriber_id: ActorId = subscriber_id.into_actor_id();
        let publisher_ids: HashSet<_> =
            zones.iter().cloned().map(crate::model::zone::actor_id_from_zone).collect();

        let channel_ref = self.subscription_ref().await?;
        channel_ref
            .send(EventSubscriptionCommand::SubscribeToPublishers { subscriber_id, publisher_ids })
            .await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn remove_subscriber(
        &self, subscriber_id: UpdateLocationsId,
    ) -> Result<(), UpdateLocationsError> {
        let subscriber_id: ActorId = subscriber_id.into_actor_id();
        let channel_ref = self.subscription_ref().await?;
        channel_ref
            .send(EventSubscriptionCommand::Unsubscribe { subscriber_id })
            .await?;
        Ok(())
    }

    async fn subscription_ref(
        &self,
    ) -> Result<EventSubscriptionChannelRef<LocationZoneBroadcastTopic>, ActorRefErr> {
        self.system
            .get_tracked_actor(self.location_subscription_actor_id.clone())
            .await
            .ok_or_else(|| ActorRefErr::NotFound(self.location_subscription_actor_id.clone()))
    }
}

#[async_trait]
impl AlertApi for UpdateLocationServices {
    async fn active_alerts(&self) -> Result<Vec<WeatherAlert>, NoaaWeatherError> {
        self.noaa.active_alerts().await
    }
}
