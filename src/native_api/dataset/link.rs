use typify::import_types;

use crate::prelude::DatasetVersion;
use crate::{
    client::{evaluate_response, BaseClient},
    identifier::Identifier,
    request::RequestType,
    response::Response,
    utils::get_dataset_id,
};

import_types!(schema = "models/message.json", struct_builder = true,);

/// Links a dataset to a specified collection.
///
/// This asynchronous function links a dataset, identified by either a persistent identifier or a numeric ID,
/// to a collection specified by its ID. It first determines the dataset's numeric ID (if not directly provided),
/// constructs the API endpoint URL for linking, and then sends a PUT request to perform the linking operation.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`,
///          representing the unique identifier of the dataset to be linked.
/// * `collection_id` - A string slice that holds the ID of the collection to which the dataset will be linked.
///
/// # Returns
///
/// A `Result` wrapping a `Response<MessageResponse>`, which contains the HTTP response status and the deserialized
/// response data indicating the outcome of the linking operation, if the request is successful, or a `String` error message on failure.
pub async fn link_dataset(
    client: &BaseClient,
    id: &Identifier,
    collection_id: &str,
) -> Result<Response<MessageResponse>, String> {
    // Determine dataset id
    let dataset_id = match id {
        Identifier::PersistentId(_) => {
            get_dataset_id(client, id, DatasetVersion::Latest.into()).await?
        }
        Identifier::Id(id) => *id,
    };

    // Endpoint metadata
    let url = format!("/api/datasets/{}/link/{}", dataset_id, collection_id);

    // Send request
    let context = RequestType::Plain;
    let response = client.put(&url, None, context, None).await;

    evaluate_response::<MessageResponse>(response).await
}

#[cfg(test)]
mod tests {
    use crate::prelude::{dataset, BaseClient, Identifier};
    use crate::test_utils::{create_test_collection, create_test_dataset, extract_test_env};

    /// Tests linking a dataset to a collection using the dataset ID.
    ///
    /// This test verifies the functionality of linking a dataset to a collection by using the dataset's ID.
    /// It involves setting up a client with API token and base URL obtained from environment variables,
    /// creating a test collection and a test dataset within a specified parent dataverse ("Root"),
    /// and then linking the dataset to the collection. The test asserts that the linking operation
    /// was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the linking operation fails,
    /// indicating an issue with the dataset linking process.
    #[tokio::test]
    async fn test_link_dataset_using_id() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a test collection
        let collection_id = create_test_collection(&client, "Root").await;

        // Create a test dataset
        let (dataset_id, _) = create_test_dataset(&client, "Root").await;

        // Link the dataset to the collection
        let response = dataset::link::link_dataset(
            &client,
            &crate::identifier::Identifier::Id(dataset_id),
            &collection_id,
        )
        .await;

        assert!(response.is_ok());
    }

    /// Tests linking a dataset to a collection using the dataset's persistent ID (PID).
    ///
    /// This test verifies the functionality of linking a dataset to a collection by using the dataset's persistent ID.
    /// It follows the same setup as the previous test but uses the persistent ID for linking the dataset to the collection.
    /// The test asserts that the linking operation was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the linking operation fails,
    /// indicating an issue with the dataset linking process.
    #[tokio::test]
    async fn test_link_dataset_using_pid() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a test collection
        let collection_id = create_test_collection(&client, "Root").await;

        // Create a test dataset
        let (_, persistent_id) = create_test_dataset(&client, "Root").await;

        // Link the dataset to the collection
        let response = dataset::link::link_dataset(
            &client,
            &Identifier::PersistentId(persistent_id),
            &collection_id,
        )
        .await;

        assert!(response.is_ok());
    }

    /// Tests linking a non-existent dataset to a collection.
    ///
    /// This test verifies that attempting to link a non-existent dataset to a collection correctly results in an error.
    /// It sets up a client using API token and base URL obtained from environment variables, creates a test collection
    /// within a specified parent dataverse ("Root"), and then attempts to link a non-existent dataset to this collection.
    /// The test asserts that the linking operation fails as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the environment variables
    /// or the API connectivity. It will also panic if the linking operation does not fail as expected, indicating
    /// an issue with error handling for non-existent datasets.
    #[tokio::test]
    async fn test_link_non_existent_dataset() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a test collection
        let collection_id = create_test_collection(&client, "Root").await;

        // Link a non-existent dataset to the collection
        let response =
            dataset::link::link_dataset(&client, &Identifier::Id(-1), &collection_id).await;

        let message = serde_json::to_string(&response).unwrap();
        assert!(message.contains("ERROR")); // Ugly, but it works
    }

    /// Tests linking a dataset to a non-existent collection.
    ///
    /// This test verifies that attempting to link a dataset to a non-existent collection correctly results in an error.
    /// It sets up a client using API token and base URL obtained from environment variables, creates a test dataset
    /// within a specified parent dataverse ("Root"), and then attempts to link this dataset to a non-existent collection.
    /// The test asserts that the linking operation fails as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the environment variables
    /// or the API connectivity. It will also panic if the linking operation does not fail as expected, indicating
    /// an issue with error handling for non-existent collections.
    #[tokio::test]
    async fn test_link_non_existent_collection() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a test dataset
        let (dataset_id, _) = create_test_dataset(&client, "Root").await;

        // Link the dataset to a non-existent collection
        let response = dataset::link::link_dataset(
            &client,
            &Identifier::Id(dataset_id),
            "non_existent_collection",
        )
        .await;

        let message = serde_json::to_string(&response).unwrap();
        assert!(message.contains("ERROR")); // Ugly, but it works
    }
}
