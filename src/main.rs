#[macro_use]
extern crate tracing;

use clap::Parser;
use settings_loader::{LoadingOptions, SettingsLoader};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = weather_coerce::setup_tracing::get_tracing_subscriber("info");
    weather_coerce::setup_tracing::init_subscriber(subscriber);

    let options = parse_options();
    let settings = load_settings(&options);
    info!("settings = {settings:?}");
    let settings = settings?;

    let server = weather_coerce::Server::build(&settings).await?;
    info!(?server, "starting server...");
    server.run_until_stopped().await.map_err(|err| err.into())
}

fn parse_options() -> weather_coerce::CliOptions {
    let options = weather_coerce::CliOptions::parse();
    if options.secrets.is_none() {
        warn!("No secrets configuration provided. Passwords (e.g., for the database) should be confined in a secret configuration and sourced in a secure manner.");
    }

    options
}

#[instrument(level = "debug")]
fn load_settings(options: &weather_coerce::CliOptions) -> anyhow::Result<weather_coerce::Settings> {
    let app_environment = std::env::var(weather_coerce::CliOptions::env_app_environment()).ok();
    if app_environment.is_none() {
        info!("No environment configuration override provided.");
    }

    weather_coerce::Settings::load(options).map_err(|err| err.into())
}
