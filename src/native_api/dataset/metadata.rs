use std::collections::HashMap;

use crate::datasetversion::determine_version;
use crate::prelude::DatasetVersion;
use crate::{
    client::{evaluate_response, BaseClient},
    identifier::Identifier,
    native_api::dataset::edit::GetDatasetResponse,
    request::RequestType,
    response::Response,
};

/// Retrieves the metadata for a dataset identified by either a persistent identifier or a numeric ID.
///
/// This asynchronous function constructs the appropriate API endpoint URL based on the type of identifier provided.
/// It then sends a GET request to the API to retrieve the metadata of the specified dataset. The function supports
/// both persistent identifiers and numeric IDs, dynamically adjusting the request parameters accordingly.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`,
///          representing the unique identifier of the dataset whose metadata is to be retrieved.
///
/// # Returns
///
/// A `Result` wrapping a `Response<GetDatasetResponse>`, which contains the HTTP response status and the deserialized
/// response data of the dataset metadata, if the request is successful, or a `String` error message on failure.
pub async fn get_dataset_meta(
    client: &BaseClient,
    id: &Identifier,
    version: &Option<DatasetVersion>,
) -> Result<Response<GetDatasetResponse>, String> {
    // Determine the version of the dataset to get
    let version = determine_version(version, client.has_api_token()).to_string();

    // Endpoint metadata
    let url = match id {
        Identifier::PersistentId(_) => format!("api/datasets/:persistentId/versions/{version}"),
        Identifier::Id(id) => format!("api/datasets/{id}/versions/{version}"),
    };

    // Send request
    let parameters = id_query_params(id);
    let context = RequestType::Plain;
    let response = client.get(url.as_str(), parameters, context, None).await;

    evaluate_response::<GetDatasetResponse>(response).await
}

/// Constructs query parameters based on the provided identifier.
///
/// This function generates a `HashMap` of query parameters if the identifier is a persistent ID.
/// If the identifier is a numeric ID, it returns `None`.
///
/// # Arguments
///
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`,
///          representing the unique identifier of the dataset.
///
/// # Returns
///
/// An `Option<HashMap<String, String>>` containing the query parameters if the identifier is a persistent ID,
/// or `None` if the identifier is a numeric ID.
pub(crate) fn id_query_params(id: &Identifier) -> Option<HashMap<String, String>> {
    match id {
        Identifier::PersistentId(id) => {
            Some(HashMap::from([("persistentId".to_string(), id.clone())]))
        }
        Identifier::Id(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::identifier::Identifier;
    use crate::prelude::{dataset, BaseClient, DatasetVersion};
    use crate::test_utils::{create_test_dataset, extract_test_env};

    /// Tests retrieval of dataset metadata by dataset ID.
    ///
    /// This test verifies that the metadata for an existing dataset can be successfully retrieved using its dataset ID.
    /// It sets up a client with API token and base URL obtained from environment variables, creates a dataset under
    /// a specified parent dataverse ("Root"), and then retrieves the metadata for this dataset. The test asserts that
    /// the retrieval request was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the environment variables
    /// or the API connectivity. It will also panic if the metadata retrieval request fails, indicating an issue
    /// with the dataset metadata retrieval process.
    #[tokio::test]
    async fn test_get_dataset_meta_by_id() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a dataset
        let (id, _) = create_test_dataset(&client, "Root").await;

        // Get the metadata
        let response = dataset::metadata::get_dataset_meta(
            &client,
            &Identifier::Id(id),
            &Some(DatasetVersion::Draft.into()),
        )
        .await
        .expect("Failed to get dataset metadata");

        assert!(response.status.is_ok())
    }

    /// Tests retrieval of dataset metadata by persistent ID.
    ///
    /// This test verifies that the metadata for an existing dataset can be successfully retrieved using its persistent ID.
    /// It follows the same setup as the previous test but uses the persistent ID for metadata retrieval. The test asserts
    /// that the retrieval request was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the environment variables
    /// or the API connectivity. It will also panic if the metadata retrieval request fails, indicating an issue
    /// with the dataset metadata retrieval process.
    #[tokio::test]
    async fn test_get_dataset_meta_by_persistent_id() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Get the metadata
        let response =
            dataset::metadata::get_dataset_meta(&client, &Identifier::PersistentId(pid), &None)
                .await
                .expect("Failed to get dataset metadata");

        assert!(response.status.is_ok())
    }

    /// Tests retrieval of dataset metadata by dataset ID for a non-existent dataset.
    ///
    /// This test attempts to retrieve metadata for a non-existent dataset using a dataset ID, verifying that the API
    /// correctly handles such requests by returning an error. It sets up a client with API token and base URL obtained
    /// from environment variables and attempts to retrieve metadata for a dataset with a known non-existent ID. The test
    /// asserts that the retrieval request fails as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the environment variables
    /// or the API connectivity. It will also panic if the metadata retrieval request does not fail as expected,
    /// indicating an issue with error handling for non-existent datasets.
    #[tokio::test]
    async fn test_get_dataset_meta_by_id_non_existent() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Get the metadata
        let response = dataset::metadata::get_dataset_meta(&client, &Identifier::Id(-1), &None)
            .await
            .expect("Failed to get dataset metadata");

        assert!(response.status.is_err())
    }

    /// Tests retrieval of dataset metadata by persistent ID for a non-existent dataset.
    ///
    /// This test attempts to retrieve metadata for a non-existent dataset using a persistent ID, verifying that the API
    /// correctly handles such requests by returning an error. It follows the same setup as the previous test but uses
    /// a known non-existent persistent ID for metadata retrieval. The test asserts that the retrieval request fails as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the environment variables
    /// or the API connectivity. It will also panic if the metadata retrieval request does not fail as expected,
    /// indicating an issue with error handling for non-existent datasets.
    #[tokio::test]
    async fn test_get_dataset_meta_by_persistent_id_non_existent() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Get the metadata
        let response = dataset::metadata::get_dataset_meta(
            &client,
            &Identifier::PersistentId("non-existent".into()),
            &None,
        )
        .await
        .expect("Failed to get dataset metadata");

        assert!(response.status.is_err())
    }
}
