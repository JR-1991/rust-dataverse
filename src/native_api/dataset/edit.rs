use std::collections::HashMap;

use serde_json;
use typify::import_types;

use crate::{
    client::{evaluate_response, BaseClient},
    request::RequestType,
    response::Response,
};

import_types!(schema = "models/dataset/edit.json", struct_builder = true);

/// Edits the metadata of a dataset identified by a persistent identifier (PID).
///
/// This asynchronous function sends a PUT request to the API endpoint designated for editing dataset metadata.
/// The function constructs the API endpoint URL dynamically, incorporating the dataset's PID. It serializes the
/// provided metadata into JSON format for the request body. Additionally, it supports a `replace` flag to determine
/// whether the existing metadata should be entirely replaced or merged with the new metadata.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `pid` - A string slice that holds the persistent identifier of the dataset whose metadata is to be edited.
/// * `replace` - A boolean flag indicating whether to replace the existing metadata (true) or merge with it (false).
/// * `body` - The `EditMetadataBody` struct instance containing the new metadata to be applied to the dataset.
///
/// # Returns
///
/// A `Result` wrapping a `Response<Dataset>`, which contains the HTTP response status and the deserialized
/// response data of the dataset after the metadata edit, if the request is successful, or a `String` error message on failure.
///
/// # Examples
///
/// Basic usage:
///
/// ```no_compile
/// # use dataverse::prelude::*;
/// # async fn run() -> Result<(), String> {
/// let api_token = "api_token".to_string();
/// let base_url = "https://demo.dataverse.com".to_string();
/// let client = BaseClient::new(&base_url, Some(&api_token))
///     .expect("Failed to create client");
///
/// let pid = "persistentId123";
/// let replace = true;
/// let body = dataset::edit::EditMetadataBody {
///     title: "Updated Dataset Title".to_string(),
///     description: "Updated description of the dataset.".to_string(),
///     // other fields...
/// };
///
/// let response = dataset::edit_dataset_metadata(&client, pid, &replace, body).await?;
///
/// println!("Dataset metadata updated: {:?}", response);
/// # Ok(())
/// # }
/// ```
pub async fn edit_dataset_metadata(
    client: &BaseClient,
    pid: &str,
    replace: &bool,
    body: EditMetadataBody,
) -> Result<Response<Dataset>, String> {
    // Endpoint metadata
    let url = "/api/datasets/:persistentId/editMetadata";

    // Build body
    let body = serde_json::to_string(&body).unwrap();

    // Build Parameters
    let parameters = Some(HashMap::from([
        ("persistentId".to_string(), pid.to_owned()),
        ("replace".to_string(), replace.to_string()),
    ]));

    // Send request
    let context = RequestType::JSON { body: body.clone() };
    let response = client.put(url, parameters, context, None).await;

    evaluate_response::<Dataset>(response).await
}

#[cfg(test)]
mod tests {
    use crate::prelude::{dataset, BaseClient};
    use crate::test_utils::{create_test_dataset, extract_test_env, prepare_edit_dataset_body};

    /// Tests the editing of dataset metadata with replacement.
    ///
    /// This test verifies that dataset metadata can be successfully edited with the replacement flag set to true.
    /// It sets up a client using API token and base URL obtained from environment variables, creates a dataset
    /// under a specified parent dataverse ("Root"), and then edits the dataset metadata with replacement.
    /// The test asserts that the edit request was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the edit request fails,
    /// indicating an issue with the dataset metadata editing process.
    #[tokio::test]
    async fn test_edit_dataset_metadata() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Edit the dataset metadata
        let body = prepare_edit_dataset_body();
        let response = dataset::edit::edit_dataset_metadata(&client, &pid, &true, body)
            .await
            .expect("Failed to edit dataset metadata");

        // Assert the request was successful
        let message = serde_json::to_string(&response).unwrap();
        assert!(response.status.is_ok(), "Response: {message}");
    }

    /// Tests the editing of dataset metadata without replacement.
    ///
    /// This test verifies that dataset metadata can be successfully edited with the replacement flag set to false.
    /// It follows the same setup as the previous test but specifies the replacement flag as false to ensure
    /// that the metadata is edited without replacing the existing metadata. The test asserts that the edit
    /// request was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the edit request fails,
    /// indicating an issue with the dataset metadata editing process.
    #[tokio::test]
    async fn test_edit_dataset_metadata_replace_false() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Edit the dataset metadata
        let body = prepare_edit_dataset_body();
        let response = dataset::edit::edit_dataset_metadata(&client, &pid, &false, body)
            .await
            .expect("Failed to edit dataset metadata");

        // Assert the request was successful
        let message = serde_json::to_string(&response).unwrap();
        assert!(response.status.is_ok(), "Response: {message}");
    }

    /// Tests the editing of dataset metadata with an invalid persistent identifier (PID).
    ///
    /// This test attempts to edit dataset metadata using an invalid PID to verify that the API
    /// correctly handles such requests by returning an error. It sets up a client using API token
    /// and base URL obtained from environment variables and attempts to edit the metadata of a dataset
    /// with a known invalid PID. The test asserts that the edit request fails as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the edit request does not fail as expected,
    /// indicating an issue with error handling for invalid PIDs.
    #[tokio::test]
    async fn test_edit_dataset_metadata_invalid_pid() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Edit the dataset metadata
        let body = prepare_edit_dataset_body();
        let response = dataset::edit::edit_dataset_metadata(&client, "invalid_pid", &true, body)
            .await
            .expect("Failed to edit dataset metadata");

        // Assert the request was successful
        assert!(response.status.is_err());
    }
}
