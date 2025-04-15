use colored::Colorize;

use crate::data_access::datafile::full_file_path;
use crate::prelude::dataset::get_dataset_meta;
use crate::prelude::{BaseClient, DatasetVersion, Identifier};
use crate::response::{Message, Status};

/// Lists the files in a dataset for a specific version.
///
/// This asynchronous function performs the following steps:
/// 1. Fetches the dataset metadata.
/// 2. Checks the response status for errors.
/// 3. Iterates over the files in the dataset metadata.
/// 4. Constructs the full file path and prints the file ID and path.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`,
///          representing the unique identifier of the dataset.
/// * `version` - An optional `DatasetVersion` specifying the version of the dataset.
///
/// # Returns
///
/// A `Result` indicating success or failure. On success, it returns `Ok(())`. On failure, it returns an error message.
///
/// # Errors
///
/// This function will return an error if:
/// - The dataset metadata cannot be fetched.
/// - The response status is an error.
/// - The dataset metadata contains no data.
/// - The dataset metadata contains no files.
/// - A file in the dataset metadata has no data file or ID.
pub async fn list_dataset_files(
    client: &BaseClient,
    id: &Identifier,
    version: &Option<DatasetVersion>,
) -> Result<(), String> {
    // Fetch metadata
    let response = get_dataset_meta(client, id, version)
        .await
        .map_err(|e| e.to_string())?;

    if let Status::ERROR = response.status {
        return Err(format!(
            "Failed to fetch dataset metadata: {}",
            response
                .message
                .unwrap_or(Message::PlainMessage("No message".to_string()))
        ));
    }

    let files = response
        .data
        .ok_or("No data found in dataset metadata".to_string())?
        .files;

    println!("{}", "Files in dataset:".bold());

    for file in files {
        let complete_path = full_file_path(file.clone());
        let fid = file
            .data_file
            .ok_or("No data file found in dataset metadata".to_string())?
            .id
            .ok_or("No ID found in data file".to_string())?;

        println!("  {} [{}]", complete_path, fid.to_string().italic());
    }

    Ok(())
}
