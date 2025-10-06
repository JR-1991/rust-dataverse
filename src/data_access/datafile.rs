//! Data file access and management functionality
//!
//! This module provides functionality for:
//! - Downloading data files from a Dataverse instance
//! - Handling different file identifier types (paths, persistent IDs, database IDs)
//! - Managing file metadata and paths
//! - Supporting resumable downloads
//! - Streaming file data

use std::error::Error;
use std::path::Path;
use std::str::FromStr;

use colored::Colorize;
use reqwest::header::{HeaderMap, HeaderValue, RANGE};

use crate::client::BaseClient;
use crate::datasetversion::DatasetVersion;
use crate::file::filestream::stream_file;
use crate::identifier::Identifier;
use crate::native_api::dataset::get_dataset_meta;
use crate::native_api::file::get_file_meta;
use crate::prelude::dataset::edit::File;
use crate::request::RequestType;
use crate::utils::validate_directory;

/// Downloads a data file to a specified output directory.
///
/// This asynchronous function performs the following steps:
/// 1. Validates the output directory.
/// 2. Determines the file path or identifier.
/// 3. Constructs the API endpoint path.
/// 4. Sends a GET request to the API to retrieve the data file.
/// 5. Streams the file to the output directory.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `file_id` - A `DataFilePath` enum instance representing the file path or identifier.
/// * `out_dir` - A `PathBuf` instance representing the output directory where the file will be saved.
/// * `ds_id` - An optional `Identifier` specifying the dataset identifier.
/// * `version` - An optional `DatasetVersion` specifying the version of the dataset.
///
/// # Returns
///
/// A `Result` indicating success or failure. On success, it returns `Ok(())`. On failure, it returns an error message.
///
/// # Errors
///
/// This function will return an error if:
/// - The output directory is invalid.
/// - The file path or identifier cannot be determined.
/// - The API request fails.
/// - The file cannot be streamed to the output directory.
pub async fn download_datafile(
    client: &BaseClient,
    file_id: &DataFilePath,
    out_dir: &Path,
    ds_id: &Option<Identifier>,
    version: &Option<DatasetVersion>,
) -> Result<(), String> {
    validate_directory(out_dir).await?;

    // Endpoint metadata
    let file_id = match file_id {
        DataFilePath::Path(path) => &get_file_id_by_path(client, ds_id, version, path).await?,
        _ => file_id,
    };

    let url = match file_id {
        DataFilePath::PersistentID(pid) => {
            format!("api/access/datafile/:persistentId?persistentId={pid}")
        }
        DataFilePath::DbIdentifier(id) => format!("api/access/datafile/{id}"),
        DataFilePath::Path(_) => {
            return Err("We should not end up here. Paths are converted to DB IDs".to_string())
        }
    };

    // If the file already exists, check how many bytes are already downloaded
    let file_path = fetch_file_path(client, file_id).await?;
    let file_size: i64 = get_file_size(client, file_id.into()).await?;
    let downloaded_bytes = get_already_downloaded_bytes(out_dir, &file_path)?;

    let headers = construct_range_header(downloaded_bytes)?;
    if file_size as u64 == downloaded_bytes.unwrap_or(0) {
        println!("\nFile already downloaded: {}\n", file_path.green().bold());
        return Ok(());
    }

    single_stream(
        client,
        out_dir,
        url,
        file_path.clone(),
        file_size,
        downloaded_bytes,
        headers,
    )
    .await
    .map_err(|e| e.to_string())
}

/// Handles streaming a single file from the Dataverse API to disk
///
/// # Arguments
///
/// * `client` - The BaseClient for making API requests
/// * `out_dir` - Output directory path
/// * `url` - API endpoint URL
/// * `file_path` - Path where file will be saved
/// * `file_size` - Expected size of file in bytes
/// * `downloaded_bytes` - Number of bytes already downloaded (for resuming)
/// * `headers` - Optional HTTP headers for the request
///
/// # Returns
///
/// Result indicating success or error
async fn single_stream(
    client: &BaseClient,
    out_dir: &Path,
    url: String,
    file_path: String,
    file_size: i64,
    downloaded_bytes: Option<u64>,
    headers: Option<HeaderMap>,
) -> Result<(), Box<dyn Error>> {
    let context = RequestType::Plain {};
    let response = client
        .get(url.as_str(), None, context, headers)
        .await
        .map_err(|e| e.to_string())?;

    stream_file(
        out_dir,
        file_path,
        response,
        file_size as u64,
        downloaded_bytes,
    )
    .await
}

/// Represents different ways to identify a data file.
#[derive(Debug)]
pub enum DataFilePath {
    /// A file path as a string.
    Path(String),
    /// A persistent identifier for the file.
    PersistentID(String),
    /// A database identifier for the file.
    DbIdentifier(i64),
}

impl FromStr for DataFilePath {
    type Err = String;

    /// Converts a string slice to a `DataFilePath`.
    ///
    /// # Arguments
    ///
    /// * `s` - A string slice that holds the file path information.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    /// - `Ok(DataFilePath)` if the string matches one of the predefined formats.
    /// - `Err(String)` if an error occurs.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("doi:") {
            Ok(DataFilePath::PersistentID(s.to_string()))
        } else if s.parse::<i64>().is_ok() {
            Ok(DataFilePath::DbIdentifier(s.parse().unwrap()))
        } else {
            Ok(DataFilePath::Path(s.to_string()))
        }
    }
}

/// Constructs the full file path from the given file metadata.
///
/// This function checks if the file has a directory label and constructs the full path accordingly.
/// If the directory label is present, it concatenates it with the file label.
/// If the directory label is not present, it returns the file label.
///
/// # Arguments
///
/// * `file` - A reference to the file metadata.
///
/// # Returns
///
/// A `String` representing the full file path.
pub(crate) fn full_file_path(file: File) -> String {
    if let Some(directory_label) = file.directory_label {
        format!(
            "{}/{}",
            directory_label,
            file.label.unwrap_or("".to_string())
        )
    } else {
        file.label.unwrap_or("".to_string())
    }
}

/// Fetches the file name based on the file identifier.
///
/// This asynchronous function determines the file name by checking the file path or fetching metadata.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `file_id` - A reference to the `DataFilePath` enum instance representing the file path or identifier.
///
/// # Returns
///
/// A `Result` indicating success or failure. On success, it returns the file name as a `String`. On failure, it returns an error message.
///
/// # Errors
///
/// This function will return an error if:
/// - The file metadata cannot be fetched.
/// - The file name cannot be determined.
pub(crate) async fn fetch_file_path(
    client: &BaseClient,
    file_id: &DataFilePath,
) -> Result<String, String> {
    let file_name = match file_id {
        DataFilePath::Path(path) => path.clone(),
        DataFilePath::PersistentID(pid) => {
            get_full_dv_path(client, &Identifier::PersistentId(pid.clone())).await?
        }
        DataFilePath::DbIdentifier(id) => get_full_dv_path(client, &Identifier::Id(*id)).await?,
    };
    Ok(file_name)
}

/// Retrieves the full Dataverse path for a file.
///
/// This asynchronous function fetches the file metadata and constructs the full path
/// by combining the directory label (if present) and the file label.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance representing the unique identifier of the file.
///
/// # Returns
///
/// A `Result` containing the full file path as a `String` on success, or an error message on failure.
///
/// # Errors
///
/// This function will return an error if:
/// - The file metadata cannot be fetched.
/// - The file label is not found in the metadata.
async fn get_full_dv_path(client: &BaseClient, id: &Identifier) -> Result<String, String> {
    let metadata = get_file_meta(client, id)
        .await?
        .data
        .ok_or("No data found in file metadata".to_string())?;
    let label = metadata
        .label
        .ok_or("No label found in file metadata".to_string())?;
    let directory_label = metadata.directory_label;

    Ok(if let Some(directory_label) = directory_label {
        format!("{}/{}", directory_label, label)
    } else {
        label
    })
}

/// Gets the number of bytes already downloaded.
///
/// This function checks if the file already exists in the output directory and returns the number of bytes already downloaded.
///
/// # Arguments
///
/// * `out_dir` - A reference to a `Path` representing the output directory.
/// * `file_name` - A reference to a `String` containing the name of the file.
///
/// # Returns
///
/// A `Result` containing an optional `u64`. On success, it returns `Some(u64)` if the file exists, otherwise `None`.
/// On failure, it returns an error message.
pub(crate) fn get_already_downloaded_bytes(
    out_dir: &Path,
    file_name: &String,
) -> Result<Option<u64>, String> {
    if out_dir.join(file_name).exists() {
        let file = std::fs::File::open(out_dir.join(file_name)).map_err(|e| e.to_string())?;
        Ok(Some(file.metadata().map_err(|e| e.to_string())?.len()))
    } else {
        Ok(None)
    }
}

/// Constructs the range header for resuming downloads.
///
/// This function creates a `HeaderMap` with the `Range` header if `downloaded_bytes` is provided.
///
/// # Arguments
///
/// * `downloaded_bytes` - An optional `u64` representing the number of bytes already downloaded.
///
/// # Returns
///
/// A `Result` containing an optional `HeaderMap`. On success, it returns `Some(HeaderMap)` if `downloaded_bytes` is provided, otherwise `None`.
/// On failure, it returns an error message.
pub(crate) fn construct_range_header(
    downloaded_bytes: Option<u64>,
) -> Result<Option<HeaderMap>, String> {
    Ok(match downloaded_bytes {
        Some(remaining_bytes) => HeaderMap::from_iter([(
            RANGE,
            HeaderValue::from_str(format!("bytes={}-", remaining_bytes).as_str())
                .map_err(|e| e.to_string())?,
        )])
        .into(),
        None => None,
    })
}

/// Retrieves the file identifier by its path.
///
/// This asynchronous function performs the following steps:
/// 1. Validates the dataset identifier.
/// 2. Fetches the dataset metadata.
/// 3. Searches for the file in the dataset metadata by its path.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `ds_id` - An optional `Identifier` specifying the dataset identifier.
/// * `version` - An optional `DatasetVersion` specifying the version of the dataset.
/// * `path` - A string representing the file path.
///
/// # Returns
///
/// A `Result` indicating success or failure. On success, it returns the file identifier as a string. On failure, it returns an error message.
///
/// # Errors
///
/// This function will return an error if:
/// - The dataset identifier is invalid.
/// - The dataset metadata cannot be fetched.
/// - The file cannot be found in the dataset metadata.
async fn get_file_id_by_path(
    client: &BaseClient,
    ds_id: &Option<Identifier>,
    version: &Option<DatasetVersion>,
    path: &str,
) -> Result<DataFilePath, String> {
    let id = ds_id
        .clone()
        .ok_or("Dataset ID is required for path-based datafile access".to_string())?;

    let metadata = get_dataset_meta(client, &id, version).await?;
    let data = metadata
        .data
        .ok_or("No data found in dataset metadata".to_string())?;

    for file in data.files {
        let complete_path = full_file_path(file.clone());
        let fid = file
            .data_file
            .ok_or("No data file found in dataset metadata".to_string())?
            .id
            .ok_or("No ID found in data file".to_string())?;

        if complete_path == path {
            return Ok(DataFilePath::DbIdentifier(fid));
        }
    }

    Err("No file found in dataset metadata".to_string())
}

/// Retrieves the file size for a given file identifier.
///
/// This asynchronous function fetches the file metadata and extracts the file size from it.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `file_id` - A reference to the `Identifier` enum instance representing the unique identifier of the file.
///
/// # Returns
///
/// A `Result` containing the file size as an `i64` on success, or an error message on failure.
///
/// # Errors
///
/// This function will return an error if:
/// - The file metadata cannot be fetched.
/// - The file metadata does not contain the necessary data.
/// - The file size is not found in the metadata.
async fn get_file_size(client: &BaseClient, file_id: Identifier) -> Result<i64, String> {
    let response = get_file_meta(client, &file_id).await?;
    let data = response
        .data
        .ok_or("No data found in file metadata".to_string())?;

    data.data_file
        .ok_or("No datafile found in file metadata".to_string())?
        .filesize
        .ok_or("No filesize found in datafile".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the `DataFilePath::from_str` function.
    #[test]
    fn test_datafile_from_str() {
        let path = "some/path/test";
        let persistent_id = "doi:10.5072/FK2/ABC123";
        let db_id = "123";

        let result = DataFilePath::from_str(path);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), DataFilePath::Path(_)));

        let result = DataFilePath::from_str(persistent_id);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), DataFilePath::PersistentID(_)));

        let result = DataFilePath::from_str(db_id);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), DataFilePath::DbIdentifier(_)));
    }
}
