//! Collection-related CLI commands for managing Dataverse collections
//!
//! This module provides commands for collection management tasks like:
//! - Creating new collections within parent dataverses
//! - Retrieving collection content and metadata
//! - Publishing collections to make them public
//! - Deleting collections from the instance

use std::path::PathBuf;

use structopt::StructOpt;
use tokio::runtime::Runtime;

use crate::client::BaseClient;
use crate::native_api::collection::create::{self, CollectionCreateBody};
use crate::native_api::collection::publish;
use crate::native_api::collection::{content, delete};

use super::base::{evaluate_and_print_response, parse_file, Matcher};

/// Subcommands for managing collections in a Dataverse instance
#[derive(StructOpt, Debug)]
#[structopt(about = "Handle collections of a Dataverse instance")]
pub enum CollectionSubCommand {
    /// Create a new collection in a parent dataverse
    #[structopt(about = "Create a collection")]
    Create {
        /// Alias of the parent dataverse where the collection will be created
        #[structopt(long, short, help = "Alias of the parent dataverse")]
        parent: String,

        /// Path to a JSON/YAML file containing the collection configuration
        #[structopt(
            long,
            short,
            help = "Path to the JSON/YAML file containing the collection body"
        )]
        body: PathBuf,
    },

    /// Get the content/metadata of a collection
    #[structopt(about = "Collection content")]
    Content {
        /// Alias of the collection to get content for
        #[structopt(help = "Alias of the collection")]
        alias: String,
    },

    /// Publish a collection, making it publicly visible
    #[structopt(about = "Publish a collection")]
    Publish {
        /// Alias of the collection to publish
        #[structopt(help = "Alias of the collection to publish")]
        alias: String,
    },

    /// Delete a collection from the Dataverse instance
    #[structopt(about = "Delete a collection")]
    Delete {
        /// Alias of the collection to delete
        #[structopt(help = "Alias of the collection to delete")]
        alias: String,
    },
}

/// Implementation of the Matcher trait for CollectionSubCommand to process collection operations
impl Matcher for CollectionSubCommand {
    /// Process the collection subcommand by matching on the variant and executing the appropriate action
    ///
    /// # Arguments
    ///
    /// * `client` - The BaseClient instance used to make API requests
    ///
    /// # Implementation Details
    ///
    /// This function handles four main operations:
    /// - Getting collection content: Retrieves metadata and content details for a collection
    /// - Creating a new collection: Creates a collection within a specified parent dataverse
    /// - Publishing a collection: Makes a collection publicly visible
    /// - Deleting a collection: Removes a collection from the Dataverse instance
    ///
    /// For each operation, it:
    /// 1. Makes the appropriate API call using the client
    /// 2. Evaluates and prints the response to the user
    fn process(self, client: &BaseClient) {
        let runtime = Runtime::new().unwrap();
        match self {
            CollectionSubCommand::Content { alias } => {
                let response = runtime.block_on(content::get_content(client, &alias));
                evaluate_and_print_response(response);
            }
            CollectionSubCommand::Create { parent, body } => {
                let body: CollectionCreateBody =
                    parse_file::<_, CollectionCreateBody>(body).expect("Failed to parse the file");
                let response =
                    runtime.block_on(create::create_collection(client, parent.as_str(), body));
                evaluate_and_print_response(response);
            }
            CollectionSubCommand::Publish { alias } => {
                let response =
                    runtime.block_on(publish::publish_collection(client, alias.as_str()));
                evaluate_and_print_response(response);
            }
            CollectionSubCommand::Delete { alias } => {
                let response = runtime.block_on(delete::delete_collection(client, &alias));
                evaluate_and_print_response(response);
            }
        };
    }
}
