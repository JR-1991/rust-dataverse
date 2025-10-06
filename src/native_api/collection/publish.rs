use typify::import_types;

use crate::{
    client::{BaseClient, evaluate_response},
    request::RequestType,
    response::Response,
};

import_types!(schema = "models/collection/create.json");

/// Publishes a collection within a dataverse.
///
/// This asynchronous function sends a POST request to the API to publish a specific collection
/// within a dataverse, identified by the collection's alias.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `alias` - A string slice that holds the alias of the collection to be published.
///
/// # Returns
///
/// A `Result` wrapping a `Response<CollectionCreateResponse>` on success, or a `String` error message on failure.
/// The `Response` object contains the HTTP response status and the deserialized response data indicating the outcome of the publish action.
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
/// let result = collection::publish_collection(&client, alias).await?;
///
/// println!("Publish result: {:?}", result);
/// # Ok(())
/// # }
/// ```
pub async fn publish_collection(
    client: &BaseClient,
    alias: &str,
) -> Result<Response<CollectionCreateResponse>, String> {
    // Endpoint metadata
    let url = format!("api/dataverses/{}/actions/:publish", alias);

    // Send request
    let context = RequestType::Plain;
    let response = client.post(url.as_str(), None, context, None).await;

    evaluate_response::<CollectionCreateResponse>(response).await
}

#[cfg(test)]
mod tests {
    use crate::prelude::{BaseClient, collection};
    use crate::test_utils::{create_test_collection, extract_test_env};

    /// Tests the publishing of an existing collection.
    ///
    /// This test verifies that an existing collection can be successfully published. It sets up a client
    /// using API token and base URL obtained from environment variables, creates a collection under a
    /// specified parent dataverse ("Root"), and then publishes the collection. The test asserts that
    /// the publish request was successful, indicating that the collection was published as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the Dataverse API.
    /// - `BASE_URL`: The base URL of the Dataverse instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the Dataverse API connectivity. It will also panic if the publish
    /// request fails, indicating an issue with the collection publishing process.
    #[tokio::test]
    async fn test_publish_collection() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token))
            .expect("Failed to create client");

        // Create a collection
        let alias = create_test_collection(&client, "Root").await;

        // Publish the collection
        let response = collection::publish::publish_collection(&client, &alias)
            .await.expect("Failed to publish collection");

        // Assert the request was successful
        assert!(response.status.is_ok());
    }

    /// Tests the publishing of a non-existent collection.
    ///
    /// This test attempts to publish a collection that does not exist to verify that the API
    /// correctly handles such requests by returning an error. It sets up a client using API token
    /// and base URL obtained from environment variables and attempts to publish a collection with
    /// a known non-existent alias. The test asserts that the publish request fails as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the Dataverse API.
    /// - `BASE_URL`: The base URL of the Dataverse instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the Dataverse API connectivity. It will also panic if the publish
    /// request does not fail as expected, indicating an issue with error handling for non-existent
    /// collections.
    #[tokio::test]
    async fn test_publish_non_existent_collection() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token))
            .expect("Failed to create client");

        // Publish a non-existent collection
        let response = collection::publish::publish_collection(&client, "non_existent_collection")
            .await.expect("Failed to publish collection");

        // Assert the request has failed
        assert!(response.status.is_err());
    }
}