use crate::model::registrar::errors::RegistrarError;
use crate::model::update::{UpdateLocationServicesRef, UpdateLocationsCommand, UpdateLocationsId};
use crate::model::zone::{LocationServicesRef, LocationZoneAggregate, LocationZoneCommand};
use crate::model::{LocationZone, LocationZoneCode, UpdateLocations};
use coerce::actor::context::ActorContext;
use coerce::actor::system::ActorSystem;
use coerce::actor::IntoActor;

#[async_trait]
pub trait RegistrarApi: Sync + Send {
    async fn initialize_forecast_zone(
        &self, zone: &LocationZoneCode, system: &ActorSystem,
    ) -> Result<(), RegistrarError>;

    async fn update_weather(
        &self, zones: &[&LocationZoneCode], ctx: &ActorContext,
    ) -> Result<Option<UpdateLocationsId>, RegistrarError>;
}

#[derive(Debug, Clone)]
pub enum RegistrarServices {
    Full(FullRegistrarServices),
    HappyPath(HappyPathServices),
}

impl RegistrarServices {
    #[allow(dead_code)]
    pub const fn full(
        location_services: LocationServicesRef, update_services: UpdateLocationServicesRef,
    ) -> Self {
        Self::Full(FullRegistrarServices::new(
            location_services,
            update_services,
        ))
    }

    #[allow(dead_code)]
    pub const fn happy() -> Self {
        Self::HappyPath(HappyPathServices)
    }
}

#[async_trait]
impl RegistrarApi for RegistrarServices {
    async fn initialize_forecast_zone(
        &self, zone: &LocationZoneCode, system: &ActorSystem,
    ) -> Result<(), RegistrarError> {
        match self {
            Self::Full(svc) => svc.initialize_forecast_zone(zone, system).await,
            Self::HappyPath(svc) => svc.initialize_forecast_zone(zone, system).await,
        }
    }

    async fn update_weather(
        &self, zones: &[&LocationZoneCode], ctx: &ActorContext,
    ) -> Result<Option<UpdateLocationsId>, RegistrarError> {
        match self {
            Self::Full(svc) => svc.update_weather(zones, ctx).await,
            Self::HappyPath(svc) => svc.update_weather(zones, ctx).await,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FullRegistrarServices {
    location_services: LocationServicesRef,
    update_services: UpdateLocationServicesRef,
}

impl FullRegistrarServices {
    pub const fn new(
        location_services: LocationServicesRef, update_services: UpdateLocationServicesRef,
    ) -> Self {
        Self { location_services, update_services }
    }

    pub async fn location_zone_for(
        &self, zone: &LocationZoneCode, system: &ActorSystem,
    ) -> Result<LocationZoneAggregate, RegistrarError> {
        let aggregate_id = zone.to_string();
        let aggregate = LocationZone::new(self.location_services.clone())
            .into_actor(Some(aggregate_id), system)
            .await?;
        Ok(aggregate)
    }
}

#[async_trait]
impl RegistrarApi for FullRegistrarServices {
    async fn initialize_forecast_zone(
        &self, zone: &LocationZoneCode, system: &ActorSystem,
    ) -> Result<(), RegistrarError> {
        let location_actor = self.location_zone_for(zone, system).await?;
        location_actor.notify(LocationZoneCommand::Subscribe(zone.clone()))?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self, ctx))]
    async fn update_weather(
        &self, zones: &[&LocationZoneCode], ctx: &ActorContext,
    ) -> Result<Option<UpdateLocationsId>, RegistrarError> {
        if zones.is_empty() {
            return Ok(None);
        }

        let zone_ids = zones.iter().copied().cloned().collect();
        let saga_id = crate::model::update::generate_id();
        let update_saga = UpdateLocations::new(self.update_services.clone())
            .into_actor(Some(saga_id.clone()), ctx.system())
            .await?;
        debug!("DMR: Update Locations saga identifier: {saga_id:?}");
        // let metadata = maplit::hashmap! { "correlation".to_string() => saga_id.id.to_string(), };
        update_saga.notify(UpdateLocationsCommand::UpdateLocations(zone_ids))?;
        Ok(Some(saga_id))
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct HappyPathServices;

#[async_trait]
impl RegistrarApi for HappyPathServices {
    #[instrument(level = "debug", skip(_system))]
    async fn initialize_forecast_zone(
        &self, _zone: &LocationZoneCode, _system: &ActorSystem,
    ) -> Result<(), RegistrarError> {
        Ok(())
    }

    #[instrument(level = "debug", skip(_ctx))]
    async fn update_weather(
        &self, _zones: &[&LocationZoneCode], _ctx: &ActorContext,
    ) -> Result<Option<UpdateLocationsId>, RegistrarError> {
        Ok(None)
    }
}
