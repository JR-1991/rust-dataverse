//! Dataset-related CLI commands for managing Dataverse datasets
//!
//! This module provides commands for dataset management tasks like:
//! - Creating, publishing, and deleting datasets
//! - Retrieving and editing dataset metadata
//! - Uploading and downloading dataset files
//! - Linking datasets between collections
//! - Managing dataset versions and file transfers

use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use colored_json::Paint;
use structopt::StructOpt;
use tokio::runtime::Runtime;

use crate::client::{print_error, BaseClient};
use crate::data_access::datafile::DataFilePath;
use crate::datasetversion::DatasetVersion;
use crate::direct_upload::upload::DirectUploadBody;
use crate::file::uploadfile::FileSource;
use crate::file::{infer_mime, UploadFile};
use crate::identifier::Identifier;
use crate::native_api::dataset;
use crate::native_api::dataset::create::{self, DatasetCreateBody};
use crate::native_api::dataset::delete;
use crate::native_api::dataset::edit;
use crate::native_api::dataset::edit::EditMetadataBody;
use crate::native_api::dataset::link;
use crate::native_api::dataset::metadata;
use crate::native_api::dataset::publish::{self, Version};
use crate::native_api::dataset::upload::{self, UploadBody};
use crate::prelude::dataset::locks::LockType;
use crate::prelude::dataset::upload::file_exists_at_dataset;
use crate::prelude::dataset::{dirupload, size};
use crate::prelude::file;
use crate::{data_access, direct_upload};

use super::base::{evaluate_and_print_response, parse_file, Matcher};

/// Subcommands for managing datasets in a Dataverse instance
#[derive(StructOpt, Debug)]
#[structopt(about = "Handle datasets of the Dataverse instance")]
pub enum DatasetSubCommand {
    /// Retrieve a dataset's metadata
    #[structopt(about = "Retrieve a datasets metadata")]
    Meta {
        #[structopt(help = "(Peristent) identifier of the dataset to retrieve")]
        id: Identifier,

        #[structopt(
            short,
            long,
            help = "Version of the dataset to retrieve. Defaults to ':latest' when there is no API token, and ':draft' when there is an API token."
        )]
        version: Option<DatasetVersion>,
    },

    /// Create a new dataset in a collection
    #[structopt(about = "Create a dataset")]
    Create {
        #[structopt(long, short, help = "Alias of the collection to create the dataset in")]
        collection: String,

        #[structopt(
            long,
            short,
            help = "Path to the JSON/YAML file containing the dataset body"
        )]
        body: PathBuf,
    },

    /// Publish a dataset version
    #[structopt(about = "Publishes a dataset")]
    Publish {
        #[structopt(help = "Persistent identifier of the dataset to publish")]
        pid: String,

        #[structopt(
            long,
            short,
            help = "Version of the dataset to publish (major, minor, updatecurrent)",
            default_value = "major"
        )]
        version: Version,
    },

    /// Delete a dataset from the Dataverse instance
    #[structopt(about = "Deletes a dataset")]
    Delete {
        #[structopt(help = "Identifier of the dataset to delete")]
        id: i64,
    },

    /// Edit dataset metadata
    #[structopt(about = "Edit the metadata of a dataset")]
    Edit {
        #[structopt(long, short, help = "Persistent identifier of the dataset to edit")]
        pid: String,

        #[structopt(
            long,
            short,
            help = "Path to the JSON/YAML file containing the metadata to edit"
        )]
        body: PathBuf,

        #[structopt(long, short, help = "Whether to replace the metadata or not")]
        replace: bool,
    },

    /// Link a dataset to another collection
    #[structopt(about = "Link a dataset to another collection")]
    Link {
        #[structopt(long, short, help = "(Persistent) identifier of the dataset to link")]
        id: Identifier,

        #[structopt(long, short, help = "Alias of the collection to link the dataset to")]
        collection: String,
    },

    /// Upload a file to a dataset
    #[structopt(about = "Upload a file to a dataset")]
    Upload {
        #[structopt(
            long,
            short,
            help = "(Persistent) Identifier of the dataset to upload the file to"
        )]
        id: Identifier,

        #[structopt(help = "Path or URL to the file to upload")]
        path: UploadFile,

        #[structopt(short, long, help = "Dataverse path to the file to upload")]
        dv_path: Option<PathBuf>,

        #[structopt(long, help = "Path to the JSON/YAML file containing the file body")]
        body: Option<PathBuf>,

        #[structopt(long, short, help = "Replace the file if it already exists")]
        replace: bool,
    },

    /// Upload a file to a dataset using direct upload
    #[structopt(about = "Upload a file to a dataset using direct upload")]
    DirectUpload {
        #[structopt(long, short, help = "Identifier of the dataset to upload the file to")]
        id: Identifier,

        #[structopt(help = "Path to the file to upload")]
        paths: Vec<PathBuf>,

        #[structopt(
            long,
            short,
            help = "Number of files to upload in parallel",
            default_value = "3"
        )]
        parallel: usize,
    },

    /// Download files from a dataset
    #[structopt(about = "Download a file from a dataset")]
    Download {
        #[structopt(
            short,
            long,
            help = "Identifier of the dataset to download the file from"
        )]
        id: Option<Identifier>,

        #[structopt(
            short,
            long,
            help = "Directory to save the file to",
            default_value = "."
        )]
        out: PathBuf,

        #[structopt(
            short,
            long,
            help = "Version of the dataset to download the file from. Defaults to ':latest' when there is no API token, and ':draft' when there is an API token."
        )]
        version: Option<DatasetVersion>,

        #[structopt(long, help = "Whether to download the entire dataset or a single file")]
        complete: bool,

        #[structopt(
            help = "Path/ID/PID to the file to download. Required when downloading a single file."
        )]
        path: Option<DataFilePath>,
    },

    /// List files in a dataset
    #[structopt(about = "List files in a dataset")]
    ListFiles {
        #[structopt(help = "Identifier of the dataset to list the files of")]
        id: Identifier,

        #[structopt(
            short,
            long,
            help = "Version of the dataset to list the files of. Defaults to ':latest' when there is no API token, and ':draft' when there is an API token."
        )]
        version: Option<DatasetVersion>,
    },

    /// Get the total size of a dataset
    #[structopt(about = "Retrieve the size of a dataset")]
    Size {
        #[structopt(help = "Identifier of the dataset to retrieve the size of")]
        id: Identifier,

        #[structopt(
            short,
            long,
            help = "Version of the dataset to retrieve the size of. Defaults to ':latest' when there is no API token, and ':draft' when there is an API token."
        )]
        version: Option<DatasetVersion>,
    },

    /// Export a dataset
    #[structopt(about = "Export a dataset to a variety of formats")]
    Export {
        #[structopt(short, long, help = "(Persistent) identifier of the dataset to export")]
        id: Identifier,

        #[structopt(
            short,
            long,
            help = "Format to use. E.g. 'ddi', 'oai_ddi', 'datacite', etc."
        )]
        format: String,

        #[structopt(short, long, help = "Path to the file to save the export to")]
        out: PathBuf,
    },

    /// Get locks for a dataset
    #[structopt(about = "Get locks for a dataset")]
    Locks {
        #[structopt(help = "Identifier of the dataset to get locks for")]
        id: Identifier,

        #[structopt(short = "t", long = "type", help = "Lock type to set to or filter by")]
        lock_type: Option<LockType>,

        #[structopt(
            short = "s",
            long = "set",
            help = "Whether to set the lock specified by '-t' on the dataset",
            conflicts_with = "remove"
        )]
        set: bool,

        #[structopt(
            short = "r",
            long = "remove",
            help = "Whether to remove the lock specified by '-t' from the dataset",
            conflicts_with = "set"
        )]
        remove: bool,
    },

    /// Submit a dataset for review
    #[structopt(about = "Submit a dataset for review")]
    Review {
        #[structopt(help = "Identifier of the dataset to submit for review")]
        id: Identifier,

        #[structopt(
            long,
            short,
            help = "Submit the dataset for review",
            conflicts_with = "reason",
            required_unless = "reason"
        )]
        submit: bool,

        #[structopt(
            long = "reason",
            short,
            help = "The reason for returning the dataset to the author",
            conflicts_with = "submit",
            required_unless = "submit"
        )]
        reason: Option<String>,
    },
}

/// Implementation of the Matcher trait for DatasetSubCommand to process dataset operations
impl Matcher for DatasetSubCommand {
    /// Process the dataset subcommand by matching on the variant and executing the appropriate action
    ///
    /// # Arguments
    /// * `client` - The BaseClient instance used to make API requests
    fn process(self, client: &BaseClient) {
        let runtime = Runtime::new().unwrap();
        match self {
            DatasetSubCommand::Meta { id, version } => {
                let response = runtime.block_on(metadata::get_dataset_meta(client, &id, &version));
                evaluate_and_print_response(response);
            }
            DatasetSubCommand::Create { collection, body } => {
                let body: DatasetCreateBody =
                    parse_file::<_, DatasetCreateBody>(body).expect("Failed to parse the file");
                let response =
                    runtime.block_on(create::create_dataset(client, &collection, body.clone()));
                evaluate_and_print_response(response);
            }
            DatasetSubCommand::Publish { pid, version } => {
                let response =
                    runtime.block_on(publish::publish_dataset(client, &pid, version.clone()));
                evaluate_and_print_response(response);
            }
            DatasetSubCommand::Delete { id } => {
                let response = runtime.block_on(delete::delete_dataset(client, &id));
                evaluate_and_print_response(response);
            }
            DatasetSubCommand::Edit { pid, body, replace } => {
                let body =
                    parse_file::<_, EditMetadataBody>(body).expect("Failed to parse the file");
                let response = runtime.block_on(edit::edit_dataset_metadata(
                    client,
                    &pid,
                    &replace,
                    body.clone(),
                ));
                evaluate_and_print_response(response);
            }
            DatasetSubCommand::Link { id, collection } => {
                let response = runtime.block_on(link::link_dataset(client, &id, &collection));
                evaluate_and_print_response(response);
            }
            DatasetSubCommand::Upload {
                id,
                path,
                body,
                replace,
                dv_path,
            } => {
                let body = Self::prepare_upload_body(body, &dv_path);
                let mut path = path;

                if let FileSource::RemoteUrl(_) = &path.file {
                    if let Some(dv_path) = &dv_path {
                        // Extract filename from dv_path and use it as the path.name
                        if let Some(filename) = dv_path.file_name().and_then(|f| f.to_str()) {
                            path.name = filename.to_string();
                        }
                    } else {
                        print_error("When uploading from a remote URL, a Dataverse path must be specified with --dv-path".to_string());
                        return;
                    }
                }

                Self::handle_file_upload(client, &runtime, &id, path, body, replace);
            }
            DatasetSubCommand::Size { id, version } => {
                let response = runtime.block_on(size::get_dataset_size(client, &id, &version));
                evaluate_and_print_response(response);
            }
            DatasetSubCommand::ListFiles { id, version } => {
                let response = runtime.block_on(dataset::list_dataset_files(client, &id, &version));

                evaluate_and_print_response(response);
            }
            DatasetSubCommand::DirectUpload {
                id,
                paths,
                parallel,
            } => {
                // We are currently using the default body for direct upload.
                // TODO: Allow custom bodies for future versions of the CLI
                let bodies = paths
                    .iter()
                    .map(Self::create_direct_upload_body)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("Failed to create direct upload bodies");

                let response = runtime.block_on(
                    direct_upload::batch_direct_upload()
                        .client(client)
                        .id(id)
                        .files(paths)
                        .bodies(bodies)
                        .hasher("MD5")
                        .n_parallel_uploads(parallel)
                        .call(),
                );

                evaluate_and_print_response(response);
            }
            DatasetSubCommand::Download {
                id,
                path: file_id,
                out,
                version,
                complete,
            } => {
                let response = if complete {
                    if let Some(id) = id {
                        runtime.block_on(data_access::dataset::download_dataset_files(
                            client,
                            id.clone(),
                            out.clone(),
                            None,
                            version.clone(),
                        ))
                    } else {
                        eprintln!("Error: Attempting to download a complete dataset without providing an identifier. Please provide an identifier with '-i' or '--id'.");
                        return;
                    }
                } else {
                    let file_id = if let Some(file_id) = file_id {
                        file_id
                    } else {
                        eprintln!("Error: Trying to download a specific file without providing a file ID. Please provide a file ID with '-p' or '--path'.");
                        return;
                    };

                    runtime.block_on(data_access::datafile::download_datafile(
                        client, &file_id, &out, &id, &version,
                    ))
                };

                if let Err(e) = response {
                    eprintln!("Error: {}", e);
                }
            }
            DatasetSubCommand::Export {
                id,
                format: exporter,
                out,
            } => {
                let response = runtime.block_on(dataset::export_dataset(client, &id, &exporter));

                // Handle the response
                let content = match response {
                    Ok(response) => match response.data {
                        Some(data) => data,
                        None => {
                            eprintln!("Error: No data returned from export");
                            return;
                        }
                    },
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        return;
                    }
                };

                // Create the output directory if it doesn't exist
                if let Some(parent) = out.parent() {
                    std::fs::create_dir_all(parent).expect("Failed to create output directory");
                }

                let mut file = File::create(&out).expect("Failed to create file");

                // Check if it is JSON-compatible and pretty print it
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&content) {
                    let pretty_json =
                        serde_json::to_string_pretty(&json).expect("Failed to pretty print JSON");
                    file.write_all(pretty_json.as_bytes())
                        .expect("Failed to write to file");
                } else {
                    file.write_all(content.as_bytes())
                        .expect("Failed to write to file");
                }

                println!("Exported dataset to {}", out.display().bold().green());
            }
            DatasetSubCommand::Locks {
                id,
                lock_type,
                set,
                remove,
            } => {
                if set {
                    if let Some(lock_type) = lock_type {
                        let response = runtime.block_on(dataset::set_lock(client, &id, lock_type));
                        evaluate_and_print_response(response);
                    } else {
                        eprintln!("Error: Lock type is required when setting a lock. Please provide a lock type with '-t' or '--type'.");
                    }
                } else if remove {
                    let response = runtime.block_on(dataset::remove_lock(client, &id, lock_type));
                    evaluate_and_print_response(response);
                } else {
                    let response = runtime.block_on(dataset::get_locks(client, &id, lock_type));
                    evaluate_and_print_response(response);
                }
            }
            DatasetSubCommand::Review { id, submit, reason } => {
                if submit {
                    let response = runtime.block_on(dataset::submit_for_review(client, &id));
                    evaluate_and_print_response(response);
                } else if let Some(reason) = reason {
                    let response =
                        runtime.block_on(dataset::return_to_author(client, &id, &reason));
                    evaluate_and_print_response(response);
                }
            }
        };
    }
}

impl DatasetSubCommand {
    /// Prepares the upload body, incorporating the Dataverse path if provided
    fn prepare_upload_body(
        body_path: Option<PathBuf>,
        dv_path: &Option<PathBuf>,
    ) -> Option<UploadBody> {
        // Parse the body file if provided
        let body = body_path
            .map(|path| parse_file::<_, UploadBody>(path).expect("Failed to parse the file"));

        // If a Dataverse path is provided, update the body with directory and filename
        // Handle the case where we have a dv_path but no body
        let body = match (body, dv_path) {
            (Some(mut upload_body), Some(path)) => {
                // If we have both body and dv_path, update the body with directory and filename
                let dir = path.parent().unwrap_or(Path::new("")).to_str().unwrap();
                let name = path.file_name().unwrap().to_str().unwrap();

                upload_body.directory_label = Some(dir.to_string());
                upload_body.filename = Some(name.to_string());
                Some(upload_body)
            }
            (None, Some(path)) => {
                // If we only have dv_path but no body, create a new default body
                let dir = path.parent().unwrap_or(Path::new("")).to_str().unwrap();
                let name = path.file_name().unwrap().to_str().unwrap();

                let upload_body = UploadBody {
                    directory_label: Some(dir.to_string()),
                    filename: Some(name.to_string()),
                    ..Default::default()
                };

                Some(upload_body)
            }
            (body, None) => body, // No dv_path, just return the original body (or None)
        };

        body
    }

    /// Checks if a file exists in a dataset and returns a tuple containing:
    /// - Whether the file exists
    /// - Whether the file has the same hash
    /// - The file ID if it exists
    ///
    /// # Arguments
    /// * `client` - The API client
    /// * `runtime` - The Tokio runtime
    /// * `id` - The dataset identifier
    /// * `version` - Optional dataset version
    /// * `path` - Path to the file to check
    ///
    /// # Returns
    /// Result containing tuple of (exists, same_hash, file_id) or an error
    fn check_file_exists(
        client: &BaseClient,
        runtime: &Runtime,
        id: &Identifier,
        version: Option<DatasetVersion>,
        file: &PathBuf,
    ) -> Result<(bool, bool, Option<i64>), Box<dyn Error>> {
        let response = runtime.block_on(file_exists_at_dataset(client, id, file, version));

        response
    }

    /// Handles uploading a file or directory to a dataset
    ///
    /// # Arguments
    /// * `client` - The API client
    /// * `runtime` - The Tokio runtime  
    /// * `id` - The dataset identifier
    /// * `path` - Path to the file/directory to upload
    /// * `body` - Optional upload metadata
    /// * `replace` - Whether to replace existing files
    fn handle_file_upload(
        client: &BaseClient,
        runtime: &Runtime,
        id: &Identifier,
        file: UploadFile,
        body: Option<UploadBody>,
        replace: bool,
    ) {
        match &file.file {
            FileSource::Path(path) => {
                if path.is_dir() {
                    Self::handle_directory_upload(client, runtime, id, path, body);
                } else {
                    Self::process_file_upload(client, runtime, id, file, body, replace)
                        .expect("Failed to process file upload");
                }
            }
            _ => Self::process_file_upload(client, runtime, id, file, body, replace)
                .expect("Failed to process file upload"),
        }
    }

    /// Handles uploading a directory and its contents to a dataset
    ///
    /// # Arguments
    /// * `client` - The API client
    /// * `runtime` - The Tokio runtime
    /// * `id` - The dataset identifier  
    /// * `path` - Path to the directory to upload
    /// * `body` - Optional upload metadata
    fn handle_directory_upload(
        client: &BaseClient,
        runtime: &Runtime,
        id: &Identifier,
        path: &Path,
        body: Option<UploadBody>,
    ) {
        let response = runtime.block_on(dirupload::upload_directory(
            client,
            id.clone(),
            path.to_str().unwrap().into(),
            body,
            None,
        ));
        evaluate_and_print_response(response);
    }

    /// Processes a single file upload, handling file existence checks and replacement logic
    ///
    /// # Arguments
    /// * `client` - The API client
    /// * `runtime` - The Tokio runtime
    /// * `id` - The dataset identifier
    /// * `path` - Path to the file to upload
    /// * `body` - Optional upload metadata
    /// * `replace` - Whether to replace an existing file
    ///
    /// # Returns
    /// Result indicating success or an error
    fn process_file_upload(
        client: &BaseClient,
        runtime: &Runtime,
        id: &Identifier,
        file: UploadFile,
        body: Option<UploadBody>,
        replace: bool,
    ) -> Result<(), Box<dyn Error>> {
        let (exists, _same_hash, file_id) = match &file.file {
            FileSource::Path(path) => {
                Self::check_file_exists(client, runtime, id, DatasetVersion::Draft.into(), path)?
            }
            _ => (false, false, None),
        };

        let response = match (exists, replace, file_id) {
            // If the file exists and we are replacing it, replace the file
            // Exists: true, Replace: true, File ID: Some(file_id)
            (true, true, Some(file_id)) => runtime.block_on(file::replace_file(
                client,
                &file_id.to_string(),
                file,
                body,
                None,
            )),
            // If the file exists and we are not replacing it, return an error
            // Exists: true, Replace: true, File ID: None
            (true, true, None) => {
                return Err("Cannot replace file, there exists no ID for this data file.".into());
            }
            // If the file does not exist, upload it
            // Exists: false, Replace: _, File ID: _
            (false, _, _) => runtime.block_on(upload::upload_file_to_dataset(
                client,
                id.clone(),
                file,
                body,
                None,
            )),
            // If the file exists and we are not replacing it, print a message and return Ok
            // Exists: true, Replace: false, File ID: _
            (true, false, _) => {
                println!(
                    "File {} exists on dataset with same name and MD5 hash. Add '--replace' or '-r' to replace it.",
                    id.to_string().bold()
                );
                return Ok(());
            }
        };

        evaluate_and_print_response(response);
        Ok(())
    }

    /// Creates a DirectUploadBody with the filename extracted from the provided path.
    ///
    /// This function extracts the filename from the path and sets it in the DirectUploadBody.
    /// It handles the default values for other fields in the body.
    ///
    /// # Arguments
    ///
    /// * `path` - A reference to the PathBuf containing the file path
    ///
    /// # Returns
    ///
    /// A DirectUploadBody with the filename set
    fn create_direct_upload_body(path: &PathBuf) -> Result<DirectUploadBody, String> {
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(String::from)
            .unwrap_or_default();

        let mime = infer_mime(path).map_err(|e| format!("Failed to infer mime type. Direct upload only supports files with known mime types. Error: {}", e))?;

        Ok(DirectUploadBody {
            file_name: Some(filename),
            mime_type: Some(mime),
            ..Default::default()
        })
    }
}
