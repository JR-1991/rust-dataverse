use crate::client::evaluate_response;
use crate::prelude::dataset::metadata::id_query_params;
use crate::prelude::dataset::upload::FileInfo;
use crate::prelude::{BaseClient, Identifier};
use crate::request::RequestType;
use crate::response::Response;

/// Retrieves the metadata for a specific file.
///
/// This asynchronous function performs the following steps:
/// 1. Constructs the API endpoint URL based on the file identifier.
/// 2. Builds the query parameters if the identifier is a persistent ID.
/// 3. Sends a GET request to the API to retrieve the file metadata.
/// 4. Evaluates the response and returns the file metadata.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`, representing the unique identifier of the file.
///
/// # Returns
///
/// A `Result` indicating success or failure. On success, it returns `Ok(Response<FileInfo>)`. On failure, it returns an error message.
///
/// # Errors
///
/// This function will return an error if:
/// - The API request fails.
/// - The response cannot be evaluated.
pub async fn get_file_meta(
    client: &BaseClient,
    id: &Identifier,
) -> Result<Response<FileInfo>, String> {
    // Endpoint metadata
    let url = match id {
        Identifier::PersistentId(_) => "api/files/:persistentId".to_string(),
        Identifier::Id(id) => format!("api/files/{id}"),
    };

    // Build Parameters
    let parameters = id_query_params(id);
    let context = RequestType::Plain;
    let response = client.get(url.as_str(), parameters, context, None).await;

    evaluate_response::<FileInfo>(response).await
}

#[cfg(test)]
mod tests {
    use crate::{
        prelude::dataset::{upload::UploadBody, upload_file_to_dataset},
        test_utils::{create_test_client, create_test_dataset},
    };

    use super::*;

    #[tokio::test]
    async fn test_get_file_meta() {
        // Arrange
        let client = create_test_client();
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Upload a file with metadata
        let body = UploadBody {
            categories: vec!["SOME_CATEGORY".to_string()],
            description: Some("SOME_DESCRIPTION".to_string()),
            directory_label: Some("SOME_DIRECTORY_LABEL".to_string()),
            ..Default::default()
        };

        let file_response = upload_file_to_dataset(
            &client,
            Identifier::PersistentId(pid.clone()),
            "tests/fixtures/file.txt",
            Some(body),
            None,
        )
        .await
        .expect("Failed to upload file");

        let files = file_response
            .data
            .expect("Failed to get file response")
            .files;

        let file = files.first().expect("Failed to get file");
        let file_id = file
            .data_file
            .as_ref()
            .expect("Failed to get file id")
            .id
            .expect("Failed to get file id");

        // Act
        let response = get_file_meta(&client, &Identifier::Id(file_id)).await;

        // Assert
        assert!(response.is_ok());

        let data = response.expect("Failed to get file metadata").data.unwrap();
        assert_eq!(data.categories, vec!["SOME_CATEGORY".to_string()]);
        assert_eq!(data.description, Some("SOME_DESCRIPTION".to_string()));
        assert_eq!(
            data.directory_label,
            Some("SOME_DIRECTORY_LABEL".to_string())
        );
    }

    #[tokio::test]
    async fn test_get_file_meta_by_db_id() {
        // Arrange
        let client = create_test_client();
        let (id, _) = create_test_dataset(&client, "Root").await;

        // Upload a file with metadata
        let body = UploadBody {
            categories: vec!["SOME_CATEGORY".to_string()],
            description: Some("SOME_DESCRIPTION".to_string()),
            directory_label: Some("SOME_DIRECTORY_LABEL".to_string()),
            ..Default::default()
        };

        let file_response = upload_file_to_dataset(
            &client,
            Identifier::Id(id),
            "tests/fixtures/file.txt",
            Some(body),
            None,
        )
        .await
        .expect("Failed to upload file");

        let files = file_response
            .data
            .expect("Failed to get file response")
            .files;

        let file = files.first().expect("Failed to get file");
        let file_id = file
            .data_file
            .as_ref()
            .expect("Failed to get file id")
            .id
            .expect("Failed to get file id");

        // Act
        let response = get_file_meta(&client, &Identifier::Id(file_id)).await;

        // Assert
        assert!(response.is_ok());

        let data = response.expect("Failed to get file metadata").data.unwrap();
        assert_eq!(data.categories, vec!["SOME_CATEGORY".to_string()]);
        assert_eq!(data.description, Some("SOME_DESCRIPTION".to_string()));
        assert_eq!(
            data.directory_label,
            Some("SOME_DIRECTORY_LABEL".to_string())
        );
    }
}
