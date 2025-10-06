//! Admin-related CLI commands for managing Dataverse instance settings
//!
//! This module provides commands for administrative tasks like:
//! - Managing storage drivers
//! - Setting/getting storage configuration for collections
//! - Resetting storage settings

use std::path::PathBuf;

use structopt::StructOpt;
use tokio::runtime::Runtime;

use crate::client::BaseClient;
use crate::native_api::admin::collection::storage;
use crate::native_api::admin::tools;

use super::base::{evaluate_and_print_response, Matcher};

/// Subcommands for administrative tasks in a Dataverse instance
#[derive(StructOpt, Debug)]
#[structopt(about = "Handle admin tasks of the Dataverse instance")]
pub enum AdminSubCommand {
    /// Get a list of available storage drivers for the Dataverse instance
    #[structopt(about = "Retrieve the storage drivers available for the Dataverse instance")]
    StorageDrivers {},

    /// Configure a specific storage driver for a collection
    #[structopt(about = "Set the storage driver for a collection")]
    SetStorage {
        /// The storage driver identifier to assign
        #[structopt(short, long, help = "Storage driver to set")]
        driver: String,
        /// Collection alias to configure storage for
        #[structopt(help = "Alias of the collection to set the storage driver for")]
        alias: String,
    },

    /// Retrieve the currently configured storage driver for a collection
    #[structopt(about = "Get the storage driver for a collection")]
    GetStorage {
        /// Collection alias to get storage config from
        #[structopt(help = "Alias of the collection to get the storage driver for")]
        alias: String,
    },

    /// Reset a collection's storage driver to the default
    #[structopt(about = "Reset the storage driver for a collection")]
    ResetStorage {
        /// Collection alias to reset storage for
        #[structopt(help = "Alias of the collection to reset the storage driver for")]
        alias: String,
    },

    /// Register an external tool
    #[structopt(about = "Registers an external tool with the Dataverse instance")]
    AddExternalTool {
        /// The tool manifest to register
        #[structopt(help = "Path to the tool manifest file")]
        manifest: PathBuf,

        /// Whether to overwrite an existing tool when it already exists. This will delete the existing tool and register a new one.
        #[structopt(
            short,
            long,
            help = "Whether to overwrite an existing tool when it already exists. This will delete the existing tool and register a new one."
        )]
        overwrite: bool,
    },

    /// List all external tools
    #[structopt(about = "Lists all external tools registered with the Dataverse instance")]
    ListExternalTools {},
}

impl Matcher for AdminSubCommand {
    /// Process admin subcommands by matching on the variant and executing the appropriate action
    ///
    /// # Arguments
    ///
    /// * `client` - The BaseClient instance used to make API requests
    ///
    /// # Implementation Details
    ///
    /// This function handles four main operations:
    /// - Listing available storage drivers
    /// - Setting a collection's storage driver
    /// - Getting a collection's storage driver
    /// - Resetting a collection's storage driver
    ///
    /// For each operation, it:
    /// 1. Makes the appropriate API call using the client
    /// 2. Evaluates and prints the response
    fn process(self, client: &BaseClient) {
        let runtime = Runtime::new().unwrap();
        match self {
            AdminSubCommand::StorageDrivers {} => {
                let response = runtime.block_on(storage::get_storage_drivers(client));
                evaluate_and_print_response(response);
            }
            AdminSubCommand::SetStorage { alias, driver } => {
                let response = runtime.block_on(storage::set_collection_storage_driver(
                    client, &alias, &driver,
                ));
                evaluate_and_print_response(response);
            }
            AdminSubCommand::GetStorage { alias } => {
                let response =
                    runtime.block_on(storage::get_collection_storage_driver(client, &alias));
                evaluate_and_print_response(response);
            }
            AdminSubCommand::ResetStorage { alias } => {
                let response =
                    runtime.block_on(storage::reset_collection_storage_driver(client, &alias));
                evaluate_and_print_response(response);
            }
            AdminSubCommand::AddExternalTool {
                manifest,
                overwrite,
            } => {
                let manifest_content = std::fs::read_to_string(manifest).unwrap();
                let manifest: tools::manifest::ExternalToolManifest =
                    serde_json::from_str(&manifest_content).unwrap();
                let response = runtime.block_on(tools::add::register_external_tool(
                    client, manifest, overwrite,
                ));
                evaluate_and_print_response(response);
            }
            AdminSubCommand::ListExternalTools {} => {
                let response = runtime.block_on(tools::list::list_external_tools(client));
                evaluate_and_print_response(response);
            }
        }
    }
}
