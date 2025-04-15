//! Information retrieval commands for Dataverse instances
//!
//! This module provides commands for retrieving metadata and information about
//! a Dataverse installation, including:
//! - Version information
//! - Server status and configuration
//! - Instance metadata

use crate::client::BaseClient;
use crate::native_api;
use structopt::StructOpt;

use super::base::{evaluate_and_print_response, Matcher};

/// Subcommands for retrieving Dataverse instance information
#[derive(StructOpt, Debug)]
#[structopt(about = "Retrieve information about the Dataverse instance")]
pub enum InfoSubCommand {
    /// Retrieves the version of the Dataverse instance
    #[structopt(about = "Retrieve the version of the Dataverse instance")]
    Version,
}

/// Implementation of command processing for info subcommands
impl Matcher for InfoSubCommand {
    /// Processes the info subcommand using the provided client
    ///
    /// # Arguments
    /// * `client` - The BaseClient for making API requests
    fn process(self, client: &BaseClient) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let response = match self {
            InfoSubCommand::Version => {
                runtime.block_on(native_api::info::version::get_version(client))
            }
        };

        evaluate_and_print_response(response);
    }
}
