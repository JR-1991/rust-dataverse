use std::collections::HashMap;

use serde_json;

use crate::file::callback::CallbackFun;
use crate::file::uploadfile::UploadFile;
use crate::{
    client::{evaluate_response, BaseClient},
    native_api::dataset::upload::{UploadBody, UploadResponse},
    request::RequestType,
    response::Response,
};

/// Replaces a file in a dataset identified by a file ID.
///
/// This asynchronous function sends a POST request to the API endpoint designated for replacing files in a dataset.
/// The function constructs the API endpoint URL dynamically, incorporating the file's ID. It sets up the request context
/// for a multipart request, including the file path, optional body metadata, and optional callbacks.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - A string slice that holds the identifier of the file to be replaced.
/// * `fpath` - A `PathBuf` instance representing the file path of the new file to be uploaded.
/// * `body` - An optional reference to an `UploadBody` struct instance containing additional metadata for the upload.
/// * `callbacks` - An optional `HashMap` of callback functions for handling events during the upload process.
///
/// # Returns
///
/// A `Result` wrapping a `Response<UploadResponse>`, which contains the HTTP response status and the deserialized
/// response data indicating the outcome of the upload operation, if the request is successful, or a `String` error message on failure.
pub async fn replace_file(
    client: &BaseClient,
    id: &str,
    file: impl Into<UploadFile>,
    body: Option<UploadBody>,
    callbacks: Option<Vec<CallbackFun>>,
) -> Result<Response<UploadResponse>, String> {
    // Endpoint metadata
    let path = format!("api/files/{}/replace", id);

    // Build hash maps and body for the request
    let file: HashMap<String, UploadFile> = HashMap::from([("file".to_string(), file.into())]);
    let callbacks = callbacks.map(|c| HashMap::from([("file".to_string(), c)]));
    let body = body
        .as_ref()
        .map(|b| HashMap::from([("jsonData".to_string(), serde_json::to_string(&b).unwrap())]));

    // Send request
    let context = RequestType::Multipart {
        bodies: body,
        files: Some(file),
        callbacks,
    };

    let response = client.post(path.as_str(), None, context, None).await;

    evaluate_response::<UploadResponse>(response).await
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        prelude::{
            dataset::upload_file_to_dataset,
            file::{get_file_meta, replace_file},
            Identifier,
        },
        test_utils::{create_test_client, create_test_dataset},
    };

    #[tokio::test]
    async fn test_replace_file() {
        // Arrange
        // Setup client and dataset
        let client = create_test_client();
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Upload a file
        let file_response = upload_file_to_dataset(
            &client,
            Identifier::PersistentId(pid.clone()),
            "tests/fixtures/file.txt",
            None,
            None,
        )
        .await
        .expect("Failed to upload file");

        // Get file id
        let file_response = file_response.data.expect("Failed to get file response");
        let file = file_response.files.first().expect("Failed to get file id");
        let file_id = file
            .data_file
            .as_ref()
            .expect("Failed to get data file")
            .id
            .expect("Failed to get file id");

        // ACT
        // Replace file
        let replace_response = replace_file(
            &client,
            &file_id.to_string(),
            "tests/fixtures/file_to_replace.txt",
            None,
            None,
        )
        .await
        .expect("Failed to replace file");

        // Assert
        assert!(replace_response.status.is_ok());

        let response = replace_response
            .data
            .expect("Failed to get replace response");
        let response = response.files.first().expect("Failed to get file id");

        let replaced_file_id = response
            .data_file
            .as_ref()
            .expect("Failed to get data file")
            .id
            .expect("Failed to get file id");

        let file_meta = get_file_meta(&client, &Identifier::Id(replaced_file_id))
            .await
            .expect("Failed to get file meta");

        let data_file = file_meta
            .data
            .expect("Failed to get file meta data")
            .data_file
            .expect("Failed to get data file");
        let expected_hash = format!(
            "{:x}",
            md5::compute(
                fs::read("tests/fixtures/file_to_replace.txt").expect("Failed to read file"),
            )
        );

        assert_eq!(
            data_file.md5.expect("Failed to get md5 from file meta"),
            expected_hash
        );
    }
}
