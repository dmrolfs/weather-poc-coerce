use crate::model::registrar::errors::RegistrarError;
use crate::model::update::{UpdateLocationsCommand, UpdateLocationsId};
use crate::model::zone::LocationZoneCommand;
use crate::model::{update, zone, LocationZoneCode};
use coerce::actor::context::ActorContext;
use coerce::actor::system::ActorSystem;
use once_cell::sync::OnceCell;
use std::sync::Arc;

#[async_trait]
pub trait RegistrarApi: Sync + Send {
    async fn initialize_forecast_zone(
        &self, zone: &LocationZoneCode, system: &ActorSystem,
    ) -> Result<(), RegistrarError>;

    async fn update_weather(
        &self, zones: &[&LocationZoneCode], ctx: &ActorContext,
    ) -> Result<Option<UpdateLocationsId>, RegistrarError>;
}

pub type RegistrarServicesRef = Arc<RegistrarServices>;

#[derive(Debug, Clone)]
pub enum RegistrarServices {
    Full(FullRegistrarServices),
    HappyPath(HappyPathServices),
}

impl RegistrarServices {
    #[allow(dead_code)]
    pub const fn full() -> Self {
        Self::Full(FullRegistrarServices)
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

static SERVICES: OnceCell<RegistrarServicesRef> = OnceCell::new();

/// Initializes the `RegistrarServices` used by the Registrar actor. This may be initialized
/// once, and will return the supplied value in an Err (i.e., `Err(services)`) on subsequent calls.
pub fn initialize_services(services: RegistrarServicesRef) -> Result<(), RegistrarServicesRef> {
    SERVICES.set(services)
}

pub fn services() -> RegistrarServicesRef {
    SERVICES.get().expect("RegistrarServices are not initialized").clone()
}

#[derive(Debug, Clone)]
pub struct FullRegistrarServices;

#[async_trait]
impl RegistrarApi for FullRegistrarServices {
    async fn initialize_forecast_zone(
        &self, zone: &LocationZoneCode, system: &ActorSystem,
    ) -> Result<(), RegistrarError> {
        let location_ref = zone::location_zone_for(zone, system).await?;
        location_ref.notify(LocationZoneCommand::Start)?;
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
        let (saga_id, update_saga) = update::update_locations_saga(ctx.system()).await?;
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
