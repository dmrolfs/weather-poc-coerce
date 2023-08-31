// use crate::model::registrar::{self, MonitoredZonesProjection, RegistrarAggregate};
// use crate::model::update::{self, UpdateLocationsProjection, UpdateLocationsSaga};
// use crate::model::zone::{self, LocationZone, LocationZoneAggregate, WeatherProjection};
// use crate::saga::{CommandRelay, EventBroadcastQuery};
use crate::server::api_errors::ApiError;
// use crate::services::noaa::{NoaaWeatherApi, NoaaWeatherServices};
use axum::extract::FromRef;
use sqlx::PgPool;
use std::fmt;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct AppState {
    // pub registrar_agg: RegistrarAggregate,
    // pub update_locations_agg: UpdateLocationsSaga,
    // pub location_agg: LocationZoneAggregate,
    // pub weather_view: WeatherProjection,
    // pub monitored_zones_view: MonitoredZonesProjection,
    // pub update_locations_view: UpdateLocationsProjection,
    pub db_pool: PgPool,
    pub location_relay_handler: Arc<JoinHandle<()>>,
    pub location_subscriber_handler: Arc<JoinHandle<()>>,
}

impl fmt::Debug for AppState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppState").finish()
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

// impl FromRef<AppState> for WeatherProjection {
//     fn from_ref(app: &AppState) -> Self {
//         app.weather_view.clone()
//     }
// }

// impl FromRef<AppState> for MonitoredZonesProjection {
//     fn from_ref(app: &AppState) -> Self {
//         app.monitored_zones_view.clone()
//     }
// }

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

#[instrument(level = "debug")]
pub async fn make_app_state(db_pool: PgPool) -> Result<AppState, ApiError> {
    // let user_agent = axum::http::HeaderValue::from_str("(here.com, contact@example.com)")
    //     .expect("invalid user_agent");
    // let base_url = Url::from_str("https://api.weather.gov")?;
    // let noaa_api = NoaaWeatherApi::new(base_url, user_agent)?;
    // let noaa = NoaaWeatherServices::Noaa(noaa_api);
    //
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
    todo!()
}
