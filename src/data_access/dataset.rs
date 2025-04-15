//! Dataset access and management functionality
//!
//! This module provides functionality for:
//! - Downloading complete datasets from a Dataverse instance
//! - Handling different dataset identifier types (persistent IDs, database IDs)
//! - Managing dataset versions and bundles
//! - Supporting file streaming and size validation
//! - Generating bundle names and cleaning identifiers

use std::collections::HashMap;
use std::path::PathBuf;

use crate::datasetversion::{determine_version, DatasetVersion};
use crate::file::filestream::stream_file;
use crate::native_api::dataset::size::get_dataset_size;
use crate::utils::validate_directory;
use crate::{client::BaseClient, identifier::Identifier, request::RequestType};

/// Downloads all files from a dataset to a specified output directory.
///
/// This asynchronous function performs the following steps:
/// 1. Validates the output directory.
/// 2. Determines the version of the dataset.
/// 3. Fetches the dataset file bundle.
/// 4. Streams the file bundle to the output directory.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`,
///          representing the unique identifier of the dataset.
/// * `out_dir` - A `PathBuf` instance representing the output directory where the files will be saved.
/// * `bundle_name` - An optional `String` containing the name of the file bundle.
/// * `version` - An optional `DatasetVersion` specifying the version of the dataset to download.
///
/// # Returns
///
/// A `Result` indicating success or failure. On success, it returns `Ok(())`. On failure, it returns an error message.
///
/// # Errors
///
/// This function will return an error if:
/// - The output directory does not exist or is not a directory.
/// - The dataset file bundle cannot be fetched.
/// - The file bundle cannot be streamed to the output directory.
pub async fn download_dataset_files(
    client: &BaseClient,
    id: Identifier,
    out_dir: PathBuf,
    bundle_name: Option<String>,
    version: Option<DatasetVersion>,
) -> Result<(), String> {
    validate_directory(&out_dir).await?;

    let (version, bundle_size) = prepare_download(client, &id, &version).await?;
    let bundle_name = bundle_name.unwrap_or_else(|| generate_bundle_name(&id, &version));

    // Endpoint metadata
    let path = match id {
        Identifier::PersistentId(_) => {
            format!("api/access/dataset/:persistentId/versions/{version}")
        }
        Identifier::Id(id) => format!("api/access/dataset/{id}"),
    };

    let context = RequestType::Plain {};
    let response = match id {
        Identifier::PersistentId(pid) => client.get(
            path.as_str(),
            Some(HashMap::from([("persistentId".to_string(), pid.clone())])),
            context,
            None,
        ),
        Identifier::Id(_) => client.get(path.as_str(), None, context, None),
    }
    .await
    .map_err(|e| {
        format!(
            "Failed to fetch file bundle from dataset: {} ({})",
            e, version
        )
    })?;

    stream_file(&out_dir, bundle_name, response, bundle_size as u64, None)
        .await
        .map_err(|e| e.to_string())
}

/// Generates a standardized name for the dataset bundle file.
///
/// # Arguments
///
/// * `id` - Reference to the dataset identifier
/// * `version` - Reference to the dataset version
///
/// # Returns
///
/// A String containing the generated bundle name in the format "dataset-{version}-{id}.zip"
fn generate_bundle_name(id: &Identifier, version: &DatasetVersion) -> String {
    format!(
        "dataset-{}-{}.zip",
        remove_special_chars(&version.to_string()),
        remove_special_chars(&id.to_string())
    )
}

/// Prepares the download by determining the dataset version and fetching the bundle size.
///
/// This asynchronous function performs the following steps:
/// 1. Determines the version of the dataset.
/// 2. Fetches the size of the dataset file bundle.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - A reference to the `Identifier` enum instance representing the unique identifier of the dataset.
/// * `version` - An optional `DatasetVersion` specifying the version of the dataset.
///
/// # Returns
///
/// A `Result` containing a tuple with the determined `DatasetVersion` and the bundle size as `i64`.
/// On failure, it returns an error message.
///
/// # Errors
///
/// This function will return an error if:
/// - The dataset size cannot be fetched.
/// - The bundle size response has no data.
/// - The bundle size response has no storage size.
async fn prepare_download(
    client: &BaseClient,
    id: &Identifier,
    version: &Option<DatasetVersion>,
) -> Result<(DatasetVersion, i64), String> {
    let version = determine_version(version, client.has_api_token());
    let bundle_size = get_dataset_size(client, id, &Some(version.clone()))
        .await
        .map_err(|e| format!("Failed to fetch dataset size: {}", e))?
        .data
        .ok_or("Bundle size response has no data".to_string())?
        .storage_size
        .ok_or("Bundle size response has no storage size".to_string())?;

    Ok((version, bundle_size))
}

/// Cleans the dataset ID by removing non-alphanumeric characters except hyphens.
///
/// This function iterates over the characters in the dataset ID and retains only alphanumeric characters and hyphens.
///
/// # Arguments
///
/// * `id` - A string slice representing the dataset ID.
///
/// # Returns
///
/// A `String` containing the cleaned dataset ID.
fn remove_special_chars(id: &str) -> String {
    id.chars()
        .filter(|c| c.is_alphanumeric() || *c == '-')
        .collect()
}
