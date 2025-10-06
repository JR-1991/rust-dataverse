use typify::import_types;

use crate::{
    client::{BaseClient, evaluate_response},
    request::RequestType,
    response::Response,
};

import_types!(schema = "models/collection/content.json");

/// Retrieves the content of a collection identified by its alias.
///
/// This asynchronous function sends a request to the API to get the content of a specific collection
/// within a dataverse, identified by the collection's alias.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `alias` - A string slice that holds the alias of the collection whose content is being requested.
///
/// # Returns
///
/// A `Result` wrapping a `Response<Vec<CollectionContent>>` on success, or a `String` error message on failure.
/// The `Response` object contains the HTTP response status and the deserialized content of the collection if the request is successful.
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
/// let alias = "my_collection";/// 
/// let content = collection::get_content(&client, alias).await?;/// 
///
/// println!("Collection content: {:?}", content);
///   
/// # Ok(()) 
/// # }
/// ```
pub async fn get_content(
    client: &BaseClient,
    alias: &str,
) -> Result<Response<Vec<CollectionContent>>, String> {
    // Endpoint metadata
    let url = format!("api/dataverses/{}/contents", alias);

    // Send request
    let context = RequestType::Plain;
    let response = client.get(url.as_str(), None, context, None).await;

    evaluate_response::<Vec<CollectionContent>>(response).await
}


#[cfg(test)]
mod tests {
    use crate::prelude::*;
    use crate::test_utils::{create_test_dataset, extract_test_env};

    /// Tests retrieval of collection content for a non-existent collection.
    ///
    /// This test verifies that attempting to retrieve the content of a non-existent collection
    /// correctly results in an error. It sets up a client using API token and base URL obtained
    /// from environment variables and makes a request for a collection known not to exist. The test
    /// asserts that the response status is an error, indicating the correct handling of non-existent
    /// collections by the API.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the request unexpectedly
    /// succeeds, indicating a failure in the API's error handling for non-existent collections.
    #[tokio::test]
    async fn test_get_collection_content_of_non_existent_collection() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token))
            .expect("Failed to create client");

        // Request a non-existent collection
        let response = collection::content::get_content(
            &client, "non_existent_collection",
        ).await.expect("Failed to get collection content");

        // Assert the request has failed
        assert!(response.status.is_err());
    }

    /// Tests retrieval of collection content for the root collection.
    ///
    /// This test verifies that the content of the root collection can be successfully retrieved.
    /// It sets up a client using API token and base URL obtained from environment variables, creates
    /// a test dataset under the root collection to ensure there is content to retrieve, and then
    /// makes a request for the root collection's content. The test asserts that the response status
    /// is successful and that the retrieved content is not empty, indicating that the API correctly
    /// returns the content of existing collections.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the request fails or
    /// if the retrieved content is unexpectedly empty, indicating a failure in the API's content
    /// retrieval process.
    #[tokio::test]
    async fn test_get_collection_content_of_root() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token))
            .expect("Failed to create client");

        create_test_dataset(&client, "Root").await;

        // Request a non-existent collection
        let response = collection::content::get_content(
            &client, "root",
        ).await.expect("Failed to get collection content");

        // Assert the request has failed
        assert!(response.status.is_ok());
        assert!(!response.data.unwrap().is_empty());
    }
}