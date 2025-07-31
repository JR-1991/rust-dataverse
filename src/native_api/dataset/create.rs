use serde_json;
use typify::import_types;

use crate::{
    client::{evaluate_response, BaseClient},
    request::RequestType,
    response::Response,
};

import_types!(schema = "models/dataset/create.json", struct_builder = true,);

/// Creates a new dataset within a specified parent dataverse.
///
/// This asynchronous function sends a POST request to the API to create a new dataset under a given parent dataverse.
/// The dataset's details are specified in the `body` parameter, which is serialized into JSON format before sending.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `parent` - A string slice that holds the identifier of the parent dataverse under which the dataset is to be created.
/// * `body` - The `DatasetCreateBody` struct instance containing the details of the dataset to be created.
///
/// # Returns
///
/// A `Result` wrapping a `Response<DatasetCreateResponse>`, which contains the HTTP response status and the deserialized
/// response data of the created dataset if the request is successful, or a `String` error message on failure.
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
/// let parent = "parent_dataverse";
/// let body = dataset::create::DatasetCreateBody {
///     title: "Example Dataset".to_string(),
///     description: "A detailed description of the example dataset.".to_string(),
///     // other fields...
/// };
///
/// let response = dataset::create_dataset(&client, parent, body).await?;
///
/// println!("Dataset created: {:?}", response);
/// # Ok(())
/// # }
/// ```
pub async fn create_dataset(
    client: &BaseClient,
    parent: &str,
    body: DatasetCreateBody,
) -> Result<Response<DatasetCreateResponse>, String> {
    // Endpoint metadata
    let url = format!("api/dataverses/{parent}/datasets");

    // Build body
    let body = serde_json::to_string(&body).unwrap();

    // Send request
    let context = RequestType::JSON { body: body.clone() };
    let response = client.post(url.as_str(), None, context, None).await;

    evaluate_response::<DatasetCreateResponse>(response).await
}

#[cfg(test)]
mod tests {
    use crate::prelude::{dataset, BaseClient};
    use crate::test_utils::{extract_test_env, prepare_dataset_body};

    /// Tests the successful creation of a dataset under a specified parent dataverse.
    ///
    /// This test function sets up a client using API token and base URL obtained from environment variables,
    /// prepares a dataset body from a fixture file, and attempts to create the dataset under a specified
    /// parent dataverse identified by "root". The test asserts that the response status is successful,
    /// indicating that the dataset was created as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the Dataverse API.
    /// - `BASE_URL`: The base URL of the Dataverse instance.
    ///
    /// # Fixture Files
    /// - `./tests/fixtures/create_dataset_body.json`: Contains the body for creating a dataset.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the Dataverse API connectivity.
    #[tokio::test]
    async fn test_create_dataset() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a dataset
        let body = prepare_dataset_body("./tests/fixtures/create_dataset_body.json".into());
        let response = dataset::create::create_dataset(&client, "root", body)
            .await
            .expect("Failed to create dataset");

        // Assert the request was successful
        assert!(response.status.is_ok());
    }

    /// Tests the creation of a dataset with an invalid body under a specified parent dataverse.
    ///
    /// This test aims to verify that attempting to create a dataset with an invalid body structure
    /// results in an error. It initializes a client using API token and base URL obtained from
    /// environment variables, prepares a dataset body from a fixture file designed to be invalid,
    /// and attempts to create the dataset under a specified parent dataverse. The test asserts that
    /// the response status indicates failure, confirming that the dataset creation process properly
    /// handles invalid body structures.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the Dataverse API.
    /// - `BASE_URL`: The base URL of the Dataverse instance.
    ///
    /// # Fixture Files
    /// - `./tests/fixtures/create_invalid_dataset_body.json`: Contains the body for creating a dataset
    ///   intended to fail due to invalid structure.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the Dataverse API connectivity.
    #[tokio::test]
    async fn test_invalid_dataset_body() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a dataset
        let body = prepare_dataset_body("./tests/fixtures/create_invalid_dataset_body.json".into());
        let response = dataset::create::create_dataset(&client, "Root", body)
            .await
            .expect("Failed to create dataset");

        // Assert the request has failed
        assert!(response.status.is_err());
    }

    /// Tests the creation of a dataset under a non-existent parent dataverse.
    ///
    /// This test verifies that attempting to create a dataset under a dataverse that does not exist
    /// results in an error. It sets up a client with environment variables for API token and base URL,
    /// prepares a dataset body from a fixture file, and attempts to create the dataset under a
    /// non-existent parent identifier. The test asserts that the response status is an error,
    /// indicating the dataset creation failed as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the Dataverse API.
    /// - `BASE_URL`: The base URL of the Dataverse instance.
    ///
    /// # Fixture Files
    /// - `./tests/fixtures/create_dataset_body.json`: Contains the body for creating a dataset.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the Dataverse API connectivity.
    #[tokio::test]
    async fn test_non_existent_parent() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a dataset
        let body = prepare_dataset_body("./tests/fixtures/create_dataset_body.json".into());
        let response = dataset::create::create_dataset(&client, "non_existent_parent", body)
            .await
            .expect("Failed to create dataset");

        // Assert the request has failed
        assert!(response.status.is_err());
    }
}
