mod cli_options;
mod http_api_settings;
#[cfg(test)]
mod tests;

pub use cli_options::CliOptions;
pub use http_api_settings::HttpApiSettings;

use settings_loader::common::database::DatabaseSettings;
use settings_loader::SettingsLoader;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Settings {
    pub http_api: HttpApiSettings,
    pub database: DatabaseSettings,
}

impl SettingsLoader for Settings {
    type Options = CliOptions;
}
