use std::path::Path;

use crate::datasetversion::DatasetVersion;
use crate::file::filestream::create_dirs_from_path;
use crate::{
    client::BaseClient, identifier::Identifier, native_api::dataset::metadata::get_dataset_meta,
};

/// Retrieves the dataset ID for a dataset identified by a persistent identifier (PID).
///
/// This asynchronous function sends a request to retrieve the metadata of a dataset using its PID.
/// It then extracts and returns the dataset ID from the metadata response.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `pid` - An `Identifier` enum instance representing the unique identifier of the dataset.
/// * `version` - An optional `DatasetVersion` specifying the version of the dataset.
///
/// # Returns
///
/// A `Result` wrapping an `i64` representing the dataset ID if the request is successful,
/// or a `String` error message on failure.
pub async fn get_dataset_id(
    client: &BaseClient,
    pid: &Identifier,
    version: Option<DatasetVersion>,
) -> Result<i64, String> {
    let response = get_dataset_meta(client, pid, &version).await?;
    match response.data {
        Some(data) => Ok(data.dataset_id.unwrap()),
        None => Err(format!("Dataset {} not found", pid)),
    }
}

/// Validates the output directory.
///
/// This function checks if the provided output directory exists and is a directory.
///
/// # Arguments
///
/// * `out_dir` - A reference to a `Path` instance representing the output directory.
///
/// # Returns
///
/// A `Result` indicating success or failure. On success, it returns `Ok(())`. On failure, it returns an error message.
///
/// # Errors
///
/// This function will return an error if:
/// - The output directory does not exist.
/// - The output directory is not a directory.
pub(crate) async fn validate_directory(out_dir: &Path) -> Result<(), String> {
    if out_dir.exists() && !out_dir.is_dir() {
        return Err(format!(
            "The output directory is not a directory: {}",
            out_dir.to_str().unwrap()
        ));
    }

    if !out_dir.exists() {
        create_dirs_from_path(out_dir)
            .await
            .map_err(|e| e.to_string())?
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validate_directory_exists() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let out_dir = temp_dir.path();

        // Act
        let result = validate_directory(out_dir).await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_directory_does_not_exist() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let out_dir = temp_dir.path();
        let parent_dir = out_dir.join("parent_dir");
        let sub_dir = parent_dir.join("sub_dir");

        // Act
        let result = validate_directory(&sub_dir).await;

        // Assert
        assert!(result.is_ok());

        // Check if the sub directory was created
        assert!(parent_dir.exists());
    }

    #[tokio::test]
    async fn test_validate_directory_is_not_a_directory() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let out_dir = temp_dir.path();
        let file_path = out_dir.join("file.txt");
        tokio::fs::write(&file_path, "Hello, world!").await.unwrap();

        // Act
        let result = validate_directory(&file_path).await;

        // Assert
        assert!(result.is_err());
    }
}
