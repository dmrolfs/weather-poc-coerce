use crate::model::registrar::{self, MonitoredZonesProjection, RegistrarAggregateSupport};
use crate::model::update::{
    self, UpdateLocationsAggregateSupport, UpdateLocationsHistoryProjection,
};
use crate::model::zone::{
    self, LocationZoneAggregateSupport, WeatherProjection, WeatherView, ZONE_OFFSET_TABLE,
    ZONE_WEATHER_TABLE, ZONE_WEATHER_VIEW,
};
use crate::model::{LocationZone, Registrar, UpdateLocations};
use crate::server::api_errors::{ApiBootstrapError, ApiError};
use crate::services::noaa::{NoaaWeatherApi, NoaaWeatherServices};
use crate::{settings, Settings};
use anyhow::anyhow;
use axum::extract::FromRef;
use coerce::actor::system::ActorSystem;
use coerce::persistent::provider::StorageProvider;
use coerce_cqrs::postgres::{
    PostgresProjectionStorage, PostgresStorageConfig, PostgresStorageProvider,
};
use coerce_cqrs::projection::processor::{ProcessorSourceProvider, ProcessorSourceRef};
use sqlx::PgPool;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
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
        let user_agent = axum::http::HeaderValue::from_str("(here.com, contact@example.com)")?;
        // .expect("invalid user_agent");
        let base_url = Url::from_str("https://api.weather.gov")?;
        let noaa_api = NoaaWeatherApi::new(base_url, user_agent)?;
        let noaa = NoaaWeatherServices::Noaa(noaa_api);

        let journal_storage_config =
            settings::storage_config_from(&settings.database, &settings.zone);
        let journal_storage =
            do_initialize_journal_storage(journal_storage_config, &system).await?;

        let registrar_support =
            Registrar::initialize_aggregate_support(journal_storage.clone(), settings, &system)
                .await?;

        let location_zone_support =
            LocationZone::initialize_aggregate_support(journal_storage.clone(), settings, &system)
                .await?;

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

        // //todo: would like to better place these connective channel and related parts;
        // // e.g., location and relay in zone module; and,  update in update module
        // let (location_tx, location_rx) = mpsc::channel(num_cpus::get());
        // let (update_tx, update_rx) = mpsc::channel(num_cpus::get());
        //
        // let location_broadcast_query: EventBroadcastQuery<LocationZone> =
        //     EventBroadcastQuery::new(num_cpus::get());
        // let location_subscriber =
        //     location_broadcast_query.subscribe(update_tx.clone(), update::location_event_to_command);
        //
        // let (update_locations_agg, update_locations_view) = update::make_update_locations_saga(
        //     location_tx,
        //     (update_tx, update_rx),
        //     &location_subscriber,
        //     noaa.clone(),
        //     db_pool.clone(),
        // )
        // .await;
        //
        // let (location_agg, weather_view) =
        //     zone::make_location_zone_aggregate_view(location_broadcast_query, noaa, db_pool.clone());
        //
        // let (registrar_agg, monitored_zones_view) = registrar::make_registrar_aggregate(
        //     db_pool.clone(),
        //     location_agg.clone(),
        //     update_locations_agg.clone(),
        // );
        //
        // let location_relay = CommandRelay::new(location_agg.clone(), location_rx);
        // let location_relay_handler = Arc::new(location_relay.run());
        // let location_subscriber_handler = Arc::new(location_subscriber.run());
        //
        // Ok(AppState {
        //     registrar_agg,
        //     update_locations_agg,
        //     location_agg,
        //     weather_view,
        //     monitored_zones_view,
        //     update_locations_view,
        //     db_pool,
        //     location_relay_handler,
        //     location_subscriber_handler,
        // })
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
