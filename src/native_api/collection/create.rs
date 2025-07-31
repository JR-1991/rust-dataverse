use serde_json;
use typify::import_types;

use crate::{
    client::{BaseClient, evaluate_response},
    request::RequestType,
    response::Response,
};

import_types!(schema = "models/collection/create.json");

/// Creates a new collection within a specified parent dataverse.
///
/// This asynchronous function sends a POST request to the API to create a new collection under a
/// given parent dataverse. The collection's details are specified in the `body` parameter, which
/// is serialized into JSON format before sending.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `parent` - A string slice that holds the identifier of the parent dataverse under which the collection is to be created.
/// * `body` - The `CollectionCreateBody` struct instance containing the details of the collection to be created.
///
/// # Returns
///
/// A `Result` wrapping a `Response<CollectionCreateResponse>`, which contains the HTTP response status and the deserialized
/// response data of the created collection if the request is successful, or a `String` error message on failure.
///
/// # Examples
///
/// ```no_compile
/// use dataverse::prelude::*;
///
/// # async fn run_example() -> Result<(), String> { ///
/// let api_token = "api_token".to_string();
/// let base_url = "https://demo.dataverse.com".to_string();
/// let client = BaseClient::new(&base_url, Some(&api_token))
///      .expect("Failed to create client");
///
/// let body = collection::create::CollectionCreateBody {
///     name: "New Collection".to_string(),
///     description: "A description of the new collection".to_string(),
///     // Other fields...
/// };
///
/// let response = collection::create_collection(&client, parent, body).await?;
///
/// println!("Created collection: {:?}", response);
/// # Ok(())
/// # }
/// ```
pub async fn create_collection(
    client: &BaseClient,
    parent: &str,
    body: CollectionCreateBody,
) -> Result<Response<CollectionCreateResponse>, String> {
    // Endpoint metadata
    let url = format!("api/dataverses/{parent}");

    // Build body
    let body = serde_json::to_string(&body).unwrap();

    // Send request
    let context = RequestType::JSON { body: body.clone() };
    let response = client.post(url.as_str(), None, context, None).await;

    evaluate_response::<CollectionCreateResponse>(response).await
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;
    use crate::test_utils::{extract_test_env, prepare_test_collection};

    /// Tests the successful creation of a collection.
    ///
    /// This test function sets up a client using API token and base URL obtained from environment variables,
    /// prepares a collection body using a utility function, and attempts to create a collection under a specified
    /// parent dataverse identified by "root". The test asserts that the response status is successful,
    /// indicating that the collection was created as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity.
    #[tokio::test]
    async fn test_create_collection() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token))
            .expect("Failed to create client");

        // Create a collection
        let body = prepare_test_collection();
        let response = collection::create::create_collection(&client, "root", body)
            .await.expect("Failed to create collection");

        // Assert the request was successful
        assert!(response.status.is_ok());
    }

    /// Tests the creation of a collection with missing contacts.
    ///
    /// This test verifies that attempting to create a collection without specifying contacts
    /// results in an error. It sets up a client using API token and base URL obtained from
    /// environment variables, prepares a collection body with the contacts field intentionally left empty,
    /// and attempts to create the collection. The test asserts that the response status indicates failure,
    /// confirming proper handling of missing required fields.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the request does not fail as expected,
    /// indicating an issue with the validation of required fields.
    #[tokio::test]
    async fn test_missing_contacts() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token))
            .expect("Failed to create client");

        // Create a collection
        let mut body = prepare_test_collection();
        body.dataverse_contacts = vec![];
        let response = collection::create::create_collection(&client, "root", body)
            .await.expect("Failed to create collection");

        // Assert the request has failed
        assert!(response.status.is_err());
    }
}
