use typify::import_types;

use crate::{
    client::{BaseClient, evaluate_response},
    request::RequestType,
    response::Response,
};

import_types!(
    schema = "models/dataset/delete.json",
    struct_builder = true,
);

/// Deletes a dataset by its ID.
///
/// This asynchronous function sends a DELETE request to the API to remove a dataset identified by its ID.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - A reference to the ID of the dataset to be deleted.
///
/// # Returns
///
/// A `Result` wrapping a `Response<UnpublishedDatasetDeleteResponse>` on success, or a `String` error message on failure.
/// The `Response` object contains the HTTP response status and the deserialized response data indicating the outcome of the delete action.
///
/// # Examples
///
/// Basic usage:
///
/// ```no_run
/// # use dataverse::prelude::*;
/// # async fn run() -> Result<(), String> {
/// let api_token = "api_token".to_string();
/// let base_url = "https://demo.dataverse.com".to_string();
/// let client = BaseClient::new(&base_url, Some(&api_token))
///     .expect("Failed to create client");
///
/// let dataset_id: i64 = 123;
///
/// let response = dataset::delete_dataset(&client, &dataset_id).await?;
///
/// println!("Dataset deletion response: {:?}", response);
/// # Ok(())
/// # }
/// ```
pub async fn delete_dataset(
    client: &BaseClient,
    id: &i64,
) -> Result<Response<UnpublishedDatasetDeleteResponse>, String> {
    // Endpoint metadata
    let url = format!("/api/datasets/{id}");

    // Send request
    let context = RequestType::Plain;
    let response = client.delete(url.as_str(), None, context, None).await;

    evaluate_response::<UnpublishedDatasetDeleteResponse>(response).await
}

#[cfg(test)]
mod tests {
    use crate::prelude::{BaseClient, dataset};
    use crate::test_utils::{create_test_dataset, extract_test_env};

    /// Tests the successful deletion of an existing dataset.
    ///
    /// This test verifies that a dataset can be successfully deleted. It sets up a client using
    /// API token and base URL obtained from environment variables, creates a dataset under a
    /// specified parent dataverse ("Root"), and then deletes the dataset. The test asserts that
    /// the deletion request was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the deletion
    /// request fails, indicating an issue with the dataset deletion process.
    #[tokio::test]
    async fn test_delete_dataset() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token))
            .expect("Failed to create client");

        // Create a dataset
        let (id, _) = create_test_dataset(&client, "Root").await;

        // Delete the dataset
        let response = dataset::delete::delete_dataset(&client, &id)
            .await.expect("Failed to delete dataset");

        // Assert the request was successful
        assert!(response.status.is_ok());
    }

    /// Tests the deletion of a non-existent dataset.
    ///
    /// This test attempts to delete a dataset that does not exist to verify that the API
    /// correctly handles such requests by returning an error. It sets up a client using API token
    /// and base URL obtained from environment variables and attempts to delete a dataset with
    /// a known non-existent ID. The test asserts that the deletion request fails as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the deletion
    /// request does not fail as expected, indicating an issue with error handling for non-existent
    /// datasets.
    #[tokio::test]
    async fn test_delete_dataset_not_found() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token))
            .expect("Failed to create client");

        // Attempt to delete a non-existent dataset
        let response = dataset::delete::delete_dataset(&client, &-1)
            .await.expect("Failed to delete dataset");

        // Assert the request was successful
        assert!(response.status.is_err());
    }
}