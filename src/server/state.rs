use crate::model::registrar::{
    MonitoredZonesProjection, RegistrarAggregateSupport, RegistrarServices,
};
use crate::model::update::{UpdateLocationsAggregateSupport, UpdateLocationsHistoryProjection};
use crate::model::zone::{LocationZoneAggregateSupport, WeatherProjection};
use crate::model::{LocationZone, Registrar, UpdateLocations};
use crate::server::api_errors::ApiBootstrapError;
use crate::services::noaa::{NoaaWeatherApi, NoaaWeatherServices};
use crate::{settings, Settings};
use anyhow::anyhow;
use axum::extract::FromRef;
use coerce::actor::system::ActorSystem;
use coerce_cqrs::postgres::{PostgresStorageConfig, PostgresStorageProvider};
use coerce_cqrs::projection::processor::{ProcessorSourceProvider, ProcessorSourceRef};
use sqlx::PgPool;
use std::fmt;
use std::str::FromStr;
use url::Url;

#[derive(Clone)]
pub struct AppState {
    pub registrar_support: RegistrarAggregateSupport,
    pub location_zone_support: LocationZoneAggregateSupport,
    pub update_locations_support: UpdateLocationsAggregateSupport,
    pub system: ActorSystem,
    pub db_pool: PgPool,
}

impl fmt::Debug for AppState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppState").finish()
    }
}

impl FromRef<AppState> for ActorSystem {
    fn from_ref(app: &AppState) -> Self {
        app.system.clone()
    }
}

// impl FromRef<AppState> for RegistrarAggregate {
//     fn from_ref(app: &AppState) -> Self {
//         app.registrar_agg.clone()
//     }
// }

// impl FromRef<AppState> for UpdateLocationsSaga {
//     fn from_ref(app: &AppState) -> Self {
//         app.update_locations_agg.clone()
//     }
// }

// impl FromRef<AppState> for LocationZoneAggregate {
//     fn from_ref(app: &AppState) -> Self {
//         app.location_agg.clone()
//     }
// }

impl FromRef<AppState> for WeatherProjection {
    fn from_ref(app: &AppState) -> Self {
        app.location_zone_support.weather_projection.clone()
    }
}

impl FromRef<AppState> for MonitoredZonesProjection {
    fn from_ref(app: &AppState) -> Self {
        app.registrar_support.monitored_zones_projection.clone()
    }
}

impl FromRef<AppState> for UpdateLocationsHistoryProjection {
    fn from_ref(app: &AppState) -> Self {
        app.update_locations_support.update_history_projection.clone()
    }
}

// impl FromRef<AppState> for UpdateLocationsProjection {
//     fn from_ref(app: &AppState) -> Self {
//         app.update_locations_agg.clone()
//     }
// }

impl FromRef<AppState> for PgPool {
    fn from_ref(app: &AppState) -> Self {
        app.db_pool.clone()
    }
}

impl AppState {
    #[instrument(level = "debug", skip(system))]
    pub async fn new(
        settings: &Settings, system: ActorSystem, db_pool: PgPool,
    ) -> Result<AppState, ApiBootstrapError> {
        let journal_storage_config =
            settings::storage_config_from(&settings.database, &settings.zone);
        let journal_storage =
            do_initialize_journal_storage(journal_storage_config, &system).await?;

        // -- registrar
        let registrar_support = Registrar::initialize_aggregate_support(
            journal_storage.clone(),
            RegistrarServices::full(),
            settings,
            &system,
        )
        .await?;

        // -- location zone
        let user_agent = axum::http::HeaderValue::from_str("(here.com, contact@example.com)")?;
        let base_url = Url::from_str("https://api.weather.gov")?;
        let noaa_api = NoaaWeatherApi::new(base_url, user_agent)?;
        let noaa = NoaaWeatherServices::Noaa(noaa_api);

        let location_zone_support = LocationZone::initialize_aggregate_support(
            journal_storage.clone(),
            noaa,
            settings,
            &system,
        )
        .await?;

        // -- update locations
        let update_locations_support = UpdateLocations::initialize_aggregate_support(
            journal_storage.clone(),
            journal_storage.clone(),
            settings,
            &system,
        )
        .await?;

        Ok(AppState {
            registrar_support,
            location_zone_support,
            update_locations_support,
            system,
            db_pool,
        })
    }
}

#[instrument(level = "trace", skip(system))]
async fn do_initialize_journal_storage(
    config: PostgresStorageConfig, system: &ActorSystem,
) -> Result<ProcessorSourceRef, ApiBootstrapError> {
    PostgresStorageProvider::connect(config, system)
        .await?
        .processor_source()
        .ok_or_else(|| anyhow!("no processor storage!"))
        .map_err(coerce_cqrs::postgres::PostgresStorageError::Storage)
        .map_err(|err| err.into())
}
