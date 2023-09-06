use crate::model::registrar::errors::RegistrarError;
use crate::model::update::{UpdateLocationServicesRef, UpdateLocationsCommand};
use crate::model::zone::{LocationServicesRef, LocationZoneAggregate, LocationZoneCommand};
use crate::model::{LocationZone, LocationZoneCode, UpdateLocations};
use coerce::actor::context::ActorContext;
use coerce::actor::IntoActor;

#[async_trait]
pub trait RegistrarApi: Sync + Send {
    async fn initialize_forecast_zone(
        &self, zone: &LocationZoneCode, ctx: &ActorContext,
    ) -> Result<(), RegistrarError>;

    async fn update_weather(
        &self, zones: &[&LocationZoneCode], ctx: &ActorContext,
    ) -> Result<(), RegistrarError>;
}

#[derive(Debug, Clone)]
pub enum RegistrarServices {
    Full(FullRegistrarServices),
    HappyPath(HappyPathServices),
}

impl RegistrarServices {
    pub const fn full(
        location_services: LocationServicesRef, update_services: UpdateLocationServicesRef,
    ) -> Self {
        Self::Full(FullRegistrarServices::new(
            location_services,
            update_services,
        ))
    }

    pub const fn happy() -> Self {
        Self::HappyPath(HappyPathServices)
    }
}

#[async_trait]
impl RegistrarApi for RegistrarServices {
    async fn initialize_forecast_zone(
        &self, zone: &LocationZoneCode, ctx: &ActorContext,
    ) -> Result<(), RegistrarError> {
        match self {
            Self::Full(svc) => svc.initialize_forecast_zone(zone, ctx).await,
            Self::HappyPath(svc) => svc.initialize_forecast_zone(zone, ctx).await,
        }
    }

    async fn update_weather(
        &self, zones: &[&LocationZoneCode], ctx: &ActorContext,
    ) -> Result<(), RegistrarError> {
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
        &self, zone: &LocationZoneCode, ctx: &ActorContext,
    ) -> Result<LocationZoneAggregate, RegistrarError> {
        let aggregate_id = zone.to_string();
        let aggregate = LocationZone::new(self.location_services.clone())
            .into_actor(Some(aggregate_id), ctx.system())
            .await?;
        Ok(aggregate)
    }
}

#[async_trait]
impl RegistrarApi for FullRegistrarServices {
    async fn initialize_forecast_zone(
        &self, zone: &LocationZoneCode, ctx: &ActorContext,
    ) -> Result<(), RegistrarError> {
        let location_actor = self.location_zone_for(zone, ctx).await?;
        location_actor.notify(LocationZoneCommand::Subscribe(zone.clone()))?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self, ctx))]
    async fn update_weather(
        &self, zones: &[&LocationZoneCode], ctx: &ActorContext,
    ) -> Result<(), RegistrarError> {
        if zones.is_empty() {
            return Ok(());
        }

        let zone_ids = zones.iter().copied().cloned().collect();
        let saga_id = crate::model::update::generate_id();
        let update_saga = UpdateLocations::new(self.update_services.clone())
            .into_actor(Some(saga_id), ctx.system())
            .await?;
        debug!("DMR: Update Locations saga identifier: {saga_id:?}");
        // let metadata = maplit::hashmap! { "correlation".to_string() => saga_id.id.to_string(), };
        update_saga.notify(UpdateLocationsCommand::UpdateLocations(zone_ids))?;
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct HappyPathServices;

#[async_trait]
impl RegistrarApi for HappyPathServices {
    #[instrument(level = "debug", skip(_ctx))]
    async fn initialize_forecast_zone(
        &self, _zone: &LocationZoneCode, _ctx: &ActorContext,
    ) -> Result<(), RegistrarError> {
        Ok(())
    }

    #[instrument(level = "debug", skip(_ctx))]
    async fn update_weather(
        &self, _zones: &[&LocationZoneCode], _ctx: &ActorContext,
    ) -> Result<(), RegistrarError> {
        Ok(())
    }
}
