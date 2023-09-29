use crate::model::registrar::{self, MonitoredZonesProjection, MonitoredZonesView};
use crate::model::update::{
    UpdateLocationsEvent, UpdateLocationsHistory, UpdateLocationsHistoryProjection,
    UpdateLocationsState,
};
use crate::model::zone::{WeatherProjection, WeatherView};
use crate::model::{LocationZone, LocationZoneCode, UpdateLocations};
use crate::server::api_errors::ApiError;
use crate::server::api_result::OptionalResult;
use crate::server::state::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{routing, Json, Router};
use coerce::actor::system::ActorSystem;
use coerce_cqrs::projection::{PersistenceId, ProjectionStorage};

#[derive(OpenApi)]
#[openapi(
    paths(
        update_weather,
        serve_update_status,
        serve_location_weather,
        serve_all_zones,
        delete_all_zones,
        add_forecast_zone,
        remove_forecast_zone,
    ),
    components(
        schemas(
            LocationZoneCode, UpdateLocationsHistory, MonitoredZonesView,
            UpdateLocationsEvent, UpdateLocationsState,
            crate::errors::WeatherError, ApiError,
        )
    ),
    tags((name = "weather", description = "Weather API"))
)]
pub struct WeatherApiDoc;

pub fn api() -> Router<AppState> {
    Router::new()
        .route("/", routing::post(update_weather))
        .route("/updates/:update_id", routing::get(serve_update_status))
        .route("/:zone", routing::get(serve_location_weather))
        .route(
            "/zones",
            routing::get(serve_all_zones).delete(delete_all_zones),
        )
        .route(
            "/zones/:zone",
            routing::post(add_forecast_zone).delete(remove_forecast_zone),
        )
}

#[utoipa::path(
    post,
    path = "/",
    context_path = "/api/v1/weather",
    tag = "weather",
    responses(
        (status = 200, description = "Initiate services update"),
        (status = "5XX", description = "server error", body = WeatherError),
    ),
)]
#[axum::debug_handler]
#[instrument(level = "debug", skip(system))]
async fn update_weather(State(system): State<ActorSystem>) -> impl IntoResponse {
    registrar::update_weather(&system)
        .await
        .map_err::<ApiError, _>(|err| err.into())
        .map(|update_saga_id| {
            update_saga_id
                .map(|id| (StatusCode::OK, id.to_string()))
                .unwrap_or_else(|| (StatusCode::OK, "".to_string()))
        })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, IntoParams, ToSchema, Serialize, Deserialize)]
#[into_params(names("update_process_id"))]
#[repr(transparent)]
#[serde(transparent)]
struct UpdateProcessId(String);

impl From<UpdateProcessId> for PersistenceId {
    fn from(id: UpdateProcessId) -> Self {
        PersistenceId::from_aggregate_id::<UpdateLocations>(id.0.as_str())
    }
}

impl std::fmt::Display for UpdateProcessId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl UpdateProcessId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl AsRef<str> for UpdateProcessId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[utoipa::path(
    get,
    path = "/updates",
    context_path = "/api/v1/weather",
    tag = "weather",
    params(UpdateProcessId),
    responses(
        (status = 200, description = "report on update weather process", body = UpdateLocationsView),
        (status = 404, description = "no update process found for identifier"),
    )
)]
#[axum::debug_handler]
async fn serve_update_status(
    Path(update_id): Path<UpdateProcessId>,
    State(view_repo): State<UpdateLocationsHistoryProjection>,
) -> impl IntoResponse {
    let view_id = update_id.into();
    view_repo
        .load_projection(&view_id)
        .await
        .map_err::<ApiError, _>(|error| error.into())
        .map(|v| OptionalResult(v.map(Json)))
}

#[utoipa::path(
    get,
    path = "/zones",
    context_path = "/api/v1/weather",
    tag = "weather",
    responses(
        (status = 200, description = "list all monitored zones", body = [MonitoredZonesView])
    ),
)]
#[instrument(level = "trace", skip(view_repo))]
async fn serve_all_zones(State(view_repo): State<MonitoredZonesProjection>) -> impl IntoResponse {
    let registrar_id = registrar::singleton_id();
    let view_id: PersistenceId = registrar_id.clone().into();

    let view = view_repo
        .load_projection(&view_id)
        .await
        .map_err::<ApiError, _>(|error| error.into())
        .map(|v| v.map(Json))
        .map(OptionalResult);

    debug!("registrar monitored zones: {view:?}");
    view
}

#[utoipa::path(
    delete,
    path = "/zones",
    context_path = "/api/v1/weather",
    tag = "weather",
    responses(
        (status = 200, description = "clear monitoring of all zones"),
    )
)]
#[instrument(level = "trace", skip(system))]
async fn delete_all_zones(State(system): State<ActorSystem>) -> impl IntoResponse {
    registrar::clear_monitoring(&system)
        .await
        .map_err::<ApiError, _>(|err| err.into())
}

#[utoipa::path(
    post,
    path = "/zones",
    context_path = "/api/v1/weather",
    tag = "weather",
    params(LocationZoneCode),
    responses(
        (status = 200, description = "add zone monitoring"),
    )
)]
#[instrument(level = "trace", skip(system))]
async fn add_forecast_zone(
    Path(zone_code): Path<LocationZoneCode>, State(system): State<ActorSystem>,
) -> impl IntoResponse {
    registrar::monitor_forecast_zone(zone_code, &system)
        .await
        .map_err::<ApiError, _>(|err| err.into())
}

#[utoipa::path(
    delete,
    path = "/zones",
    context_path = "/api/v1/weather",
    tag = "weather",
    params(LocationZoneCode),
    responses(
        (status = 200, description = "remove zone monitoring"),
    )
)]
#[instrument(level = "trace", skip(system))]
async fn remove_forecast_zone(
    Path(zone_code): Path<LocationZoneCode>, State(system): State<ActorSystem>,
) -> impl IntoResponse {
    registrar::forget_forecast_zone(zone_code, &system)
        .await
        .map_err::<ApiError, _>(|err| err.into())
}

#[utoipa::path(
    get,
    path = "/",
    context_path = "/api/v1/weather",
    tag = "weather",
    params(
        ("zone_code" = String, Path, description = "Location Zone Code"),
    ),
    responses(
        (status = 200, description = "Location Weather Report", body = WeatherView),
        (status = 404, description = "location zone not found"),
    )
)]
#[axum::debug_handler]
#[instrument(level = "debug", skip(view_repo))]
async fn serve_location_weather(
    Path(zone_code): Path<LocationZoneCode>, State(view_repo): State<WeatherProjection>,
) -> impl IntoResponse {
    let view_id = PersistenceId::from_aggregate_id::<LocationZone>(zone_code.as_ref());
    let view: Result<Option<WeatherView>, ApiError> = view_repo
        .load_projection(&view_id)
        .await
        .map_err(|err| err.into());
    debug!("location {zone_code} weather: {view:?}");

    view
        .map(|ov| ov.map(Json))
        .map(OptionalResult)
}
