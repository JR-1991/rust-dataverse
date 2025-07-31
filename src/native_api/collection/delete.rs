use typify::import_types;

use crate::{
    client::{BaseClient, evaluate_response},
    request::RequestType,
    response::Response,
};

import_types!(schema = "models/collection/delete.json");

/// Deletes a collection identified by its alias.
///
/// This asynchronous function sends a DELETE request to the API to remove a specific collection
/// within a dataverse, identified by the collection's alias.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `alias` - A string slice that holds the alias of the collection to be deleted.
///
/// # Returns
///
/// A `Result` wrapping a `Response<CollectionDeleteResponse>` on success, or a `String` error message on failure.
/// The `Response` object contains the HTTP response status and the deserialized response data indicating the outcome of the deletion.
///
/// # Examples
///
/// ```no_run
/// use dataverse::prelude::*;
///
/// # async fn run_example() -> Result<(), String> {
/// let api_token = "api_token".to_string();
/// let base_url = "https://demo.dataverse.com".to_string();
/// let client = BaseClient::new(&base_url, Some(&api_token))
///     .expect("Failed to create client");
///
/// let alias = "my_collection";
/// let result = collection::delete_collection(&client, alias).await?;
///
/// println!("Deletion result: {:?}", result);
/// # Ok(())
/// # }
/// ```
pub async fn delete_collection(
    client: &BaseClient,
    alias: &str,
) -> Result<Response<CollectionDeleteResponse>, String> {
    // Endpoint metadata
    let url = format!("/api/dataverses/{alias}");

    // Send request
    let context = RequestType::Plain;
    let response = client.delete(url.as_str(), None, context, None).await;

    evaluate_response::<CollectionDeleteResponse>(response).await
}


#[cfg(test)]
mod tests {
    use crate::prelude::*;
    use crate::test_utils::{create_test_collection, extract_test_env};

    /// Tests the deletion of an existing collection.
    ///
    /// This test verifies that a collection can be successfully deleted. It sets up a client using
    /// API token and base URL obtained from environment variables, creates a collection under a
    /// specified parent dataverse ("Root"), and then deletes the collection. The test asserts that
    /// the deletion request was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the Dataverse API.
    /// - `BASE_URL`: The base URL of the Dataverse instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the Dataverse API connectivity. It will also panic if the deletion
    /// request fails, indicating an issue with the collection deletion process.
    #[tokio::test]
    async fn test_delete_collection() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token))
            .expect("Failed to create client");

        // Create a collection
        let alias = create_test_collection(&client, "Root").await;

        // Delete the collection
        let response = collection::delete::delete_collection(&client, &alias)
            .await.expect("Failed to delete collection");

        // Assert the request was successful
        assert!(response.status.is_ok());
    }

    /// Tests the deletion of a non-existent collection.
    ///
    /// This test attempts to delete a collection that does not exist to verify that the API
    /// correctly handles such requests by returning an error. It sets up a client using API token
    /// and base URL obtained from environment variables and attempts to delete a collection with
    /// a known non-existent alias. The test asserts that the deletion request fails as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the Dataverse API.
    /// - `BASE_URL`: The base URL of the Dataverse instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the Dataverse API connectivity. It will also panic if the deletion
    /// request does not fail as expected, indicating an issue with error handling for non-existent
    /// collections.
    #[tokio::test]
    async fn test_delete_non_existent_collection() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token))
            .expect("Failed to create client");

        // Delete a non-existent collection
        let response = collection::delete::delete_collection(&client, "non_existent_collection")
            .await.expect("Failed to delete collection");

        // Assert the request has failed
        assert!(response.status.is_err());
    }
}