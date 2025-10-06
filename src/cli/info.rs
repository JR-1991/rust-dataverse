//! Information retrieval commands for Dataverse instances
//!
//! This module provides commands for retrieving metadata and information about
//! a Dataverse installation, including:
//! - Version information
//! - Server status and configuration
//! - Instance metadata

use crate::client::BaseClient;
use crate::native_api;
use colored::Colorize;
use lazy_static::lazy_static;
use structopt::StructOpt;

use super::base::{evaluate_and_print_response, Matcher};

lazy_static! {
    static ref EXPORTERS_INSTRUCTIONS: String = format!(
        "
You can use the key to export a dataset using the following command:

{}\n",
        "dvcli dataset export --id <dataset-id> --format <exporter-key> --out <output-file>"
            .bold()
            .blue()
    );
}

/// Subcommands for retrieving Dataverse instance information
#[derive(StructOpt, Debug)]
#[structopt(about = "Retrieve information about the Dataverse instance")]
pub enum InfoSubCommand {
    /// Retrieves the version of the Dataverse instance
    #[structopt(about = "Retrieve the version of the Dataverse instance")]
    Version,

    /// Retrieves the exporters of the Dataverse instance
    #[structopt(about = "Retrieve the exporters of the Dataverse instance")]
    Exporters,
}

/// Implementation of command processing for info subcommands
impl Matcher for InfoSubCommand {
    /// Processes the info subcommand using the provided client
    ///
    /// # Arguments
    /// * `client` - The BaseClient for making API requests
    fn process(self, client: &BaseClient) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        match self {
            InfoSubCommand::Version => {
                let response = runtime.block_on(native_api::info::version::get_version(client));
                evaluate_and_print_response(response);
            }
            InfoSubCommand::Exporters => {
                let response = runtime.block_on(native_api::info::exporters::get_exporters(client));

                match response {
                    Ok(response) => {
                        let exporters = response.data;

                        if let Some(exporters) = exporters {
                            println!("\n{}", "Exporters:".bold().blue());
                            for (key, exporter) in exporters.iter() {
                                println!("  - {}: {}", key.bold().blue(), exporter.display_name);
                            }
                            println!("{}", EXPORTERS_INSTRUCTIONS.to_string());
                        } else {
                            eprintln!("Error: {}", response.message.unwrap());
                        }
                    }
                    Err(e) => eprintln!("Error: {:?}", e),
                }
            }
        };
    }
}
