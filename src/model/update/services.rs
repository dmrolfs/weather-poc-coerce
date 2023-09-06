use crate::connect::event_broadcast::{EventBroadcastChannel, EventChannelCommand};
use crate::model::update::{LocationZoneBroadcastTopic, UpdateLocationsId};
use crate::model::{LocationZoneCode, WeatherAlert};
use crate::services::noaa::{AlertApi, NoaaWeatherError, NoaaWeatherServices};
use coerce::actor::{ActorId, LocalActorRef};
use std::collections::HashSet;
use std::sync::Arc;

pub type UpdateLocationServicesRef = Arc<UpdateLocationServices>;

#[derive(Debug, Clone)]
pub struct UpdateLocationServices {
    location_subscriber: LocalActorRef<EventBroadcastChannel<LocationZoneBroadcastTopic>>,
    noaa: NoaaWeatherServices,
}

impl UpdateLocationServices {
    pub fn new(
        location_subscriber: LocalActorRef<EventBroadcastChannel<LocationZoneBroadcastTopic>>,
        noaa: NoaaWeatherServices,
    ) -> Self {
        Self { location_subscriber, noaa }
    }

    pub async fn add_subscriber(
        &self, subscriber_id: UpdateLocationsId, zones: &[LocationZoneCode],
    ) {
        let subscriber_id: ActorId = subscriber_id.id.into();
        let publisher_ids: HashSet<_> = zones.iter().map(|z| ActorId::from(z.as_ref())).collect();
        let outcome = self
            .location_subscriber
            .send(EventChannelCommand::SubscribeToPublishers {
                subscriber_id: subscriber_id.clone(),
                publisher_ids,
            })
            .await;

        if let Err(error) = outcome {
            error!(?error, "failed to register update locations saga, {subscriber_id}, for location event broadcasts.");
        }
    }

    pub async fn remove_subscriber(&self, subscriber_id: UpdateLocationsId) {
        let subscriber_id: ActorId = subscriber_id.id.into();
        let outcome = self
            .location_subscriber
            .send(EventChannelCommand::Unsubscribe { subscriber_id: subscriber_id.clone() })
            .await;

        if let Err(error) = outcome {
            error!(?error, "failed to unsubscribe update locations saga, {subscriber_id}, from location event broadcasts.")
        }
    }
}

#[async_trait]
impl AlertApi for UpdateLocationServices {
    async fn active_alerts(&self) -> Result<Vec<WeatherAlert>, NoaaWeatherError> {
        self.noaa.active_alerts().await
    }
}
