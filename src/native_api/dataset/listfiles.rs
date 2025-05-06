use std::collections::HashMap;

use crate::data_access::datafile::full_file_path;
use crate::prelude::dataset::get_dataset_meta;
use crate::prelude::{BaseClient, DatasetVersion, Identifier};
use crate::response::{Message, Status};

use super::edit::File;

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
) -> Result<HashMap<String, File>, String> {
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

    Ok(files
        .iter()
        .map(|file| {
            let complete_path = full_file_path(file.clone());
            (complete_path, file.clone())
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::dataset::upload::UploadBody;
    use crate::prelude::dataset::upload_file_to_dataset;
    use crate::prelude::Identifier;
    use crate::test_utils::{create_test_client, create_test_dataset};

    #[tokio::test]
    async fn test_list_dataset_files() {
        // Arrange
        let client = create_test_client();
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Upload a file
        let body = UploadBody {
            directory_label: Some("test".to_string()),
            ..Default::default()
        };

        upload_file_to_dataset(
            &client,
            Identifier::PersistentId(pid.clone()),
            "tests/fixtures/file.txt",
            Some(body),
            None,
        )
        .await
        .expect("Failed to upload file");

        // Act
        let result = list_dataset_files(&client, &Identifier::PersistentId(pid), &None)
            .await
            .expect("Failed to list dataset files");

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result.keys().next().unwrap(), "test/file.txt");
    }
}
