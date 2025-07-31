//! File-related CLI commands for managing Dataverse files
//!
//! This module provides commands for file management tasks like:
//! - Retrieving file metadata
//! - Replacing existing files
//! - Downloading files from datasets

use std::path::PathBuf;

use structopt::StructOpt;

use crate::data_access;
use crate::data_access::datafile::DataFilePath;
use crate::native_api::file::replace;
use crate::prelude::file::metadata;
use crate::prelude::{DatasetVersion, Identifier};
use crate::response::{Message, Response, Status};
use crate::{client::BaseClient, native_api::dataset::upload::UploadBody};

use super::base::{evaluate_and_print_response, parse_file, Matcher};

/// Subcommands for managing files in a Dataverse instance
#[derive(StructOpt, Debug)]
#[structopt(about = "Handle files of a Dataverse instance")]
pub enum FileSubCommand {
    /// Retrieves metadata for a specific file
    #[structopt(about = "Get file metadata")]
    Meta {
        #[structopt(help = "Identifier of the file to get metadata for")]
        id: Identifier,
    },

    /// Replaces an existing file with a new version
    #[structopt(about = "Replace a file")]
    Replace {
        #[structopt(help = "Path to the file to replace")]
        path: PathBuf,

        #[structopt(long, short, help = "Identifier of the of the file to replace")]
        id: String,

        #[structopt(
            long,
            short,
            help = "Path to the JSON/YAML file containing the file body"
        )]
        body: Option<PathBuf>,

        #[structopt(long, short, help = "Force the replacement of the file")]
        force: bool,
    },

    /// Downloads a file from a dataset
    #[structopt(about = "Download a file")]
    Download {
        #[structopt(help = "Identifier of the file to download")]
        file_id: DataFilePath,

        #[structopt(short, long, help = "Path to save the file to")]
        path: PathBuf,

        #[structopt(short, long, help = "Version of the dataset to download the file from")]
        version: Option<DatasetVersion>,
    },
}

/// Implementation of command processing for file-related operations
impl Matcher for FileSubCommand {
    /// Processes the file subcommand using the provided client
    ///
    /// # Arguments
    /// * `client` - The BaseClient for making API requests
    fn process(self, client: &BaseClient) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        match self {
            FileSubCommand::Replace {
                id,
                path,
                body,
                force,
            } => {
                let body = prepare_replace_body(&body, &force);
                let response =
                    runtime.block_on(replace::replace_file(client, &id, path.clone(), body, None));

                evaluate_and_print_response(response);
            }
            FileSubCommand::Meta { id } => {
                println!("Getting file metadata for {id:?}");
                let response = runtime.block_on(metadata::get_file_meta(client, &id));
                evaluate_and_print_response(response);
            }
            FileSubCommand::Download {
                file_id: id,
                path,
                version,
            } => {
                let response = runtime.block_on(data_access::download_datafile(
                    client, &id, &path, &None, &version,
                ));

                // In order to use the `evaluate_and_print_response` function,
                // we need to wrap the response in an `Ok`
                // This is a bit of a hack, but it works for now
                let response: Response<()> = match response {
                    Ok(_) => Response::from_message(
                        Message::PlainMessage("File downloaded successfully".to_string()),
                        Status::OK,
                    ),
                    Err(e) => Response::from_message(
                        Message::PlainMessage(format!("Error downloading file: {e}")),
                        Status::ERROR,
                    ),
                };

                evaluate_and_print_response(Ok(response));
            }
        };
    }
}

/// Prepares the upload body for file replacement operations
///
/// # Arguments
/// * `body` - Optional path to a JSON/YAML file containing upload metadata
/// * `force` - Whether to force the file replacement
///
/// # Returns
/// Optional UploadBody containing the parsed metadata and force flag
fn prepare_replace_body(body: &Option<PathBuf>, force: &bool) -> Option<UploadBody> {
    match body {
        Some(body) => {
            let mut body = parse_file::<_, UploadBody>(body).unwrap();
            if body.force_replace.is_none() {
                body.force_replace = Some(force.to_owned());
            }
            Some(body)
        }
        _ => None,
    }
}
