use std::{collections::HashMap, str::FromStr};

use serde::{Deserialize, Serialize};
use typify::import_types;

use crate::{
    client::{BaseClient, evaluate_response},
    request::RequestType,
    response::Response,
};

import_types!(
    schema = "models/dataset/publish.json",
    struct_builder = true,
);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Version {
    #[serde(rename = "major")]
    Major,

    #[serde(rename = "minor")]
    Minor,

    #[serde(rename = "updatecurrent")]
    UpdateCurrent,
}

impl FromStr for Version {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "major" => Ok(Version::Major),
            "minor" => Ok(Version::Minor),
            "updatecurrent" => Ok(Version::UpdateCurrent),
            _ => Err(format!("Invalid version: {s}")),
        }
    }
}

/// Publishes a dataset identified by a persistent identifier (PID) with a specified version type.
///
/// This asynchronous function sends a POST request to the API endpoint designated for publishing datasets.
/// The function constructs the API endpoint URL dynamically, incorporating the dataset's PID and the version type.
/// It sets up the request parameters and context, and then sends the request to publish the dataset.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `pid` - A string slice that holds the persistent identifier of the dataset to be published.
/// * `version` - A `Version` enum instance representing the type of version update (major, minor, or update current).
///
/// # Returns
///
/// A `Result` wrapping a `Response<DatasetPublishResponse>`, which contains the HTTP response status and the deserialized
/// response data of the dataset publishing operation, if the request is successful, or a `String` error message on failure.
///
/// # Examples
///
/// Basic usage:
///
/// ```no_run
/// use dataverse::prelude::*;
/// use dataverse::client::BaseClient;
/// use dataverse::native_api::dataset::publish::{publish_dataset, Version};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let api_token = "api_token".to_string();
/// let base_url = "https://demo.dataverse.com".to_string();
/// let client = BaseClient::new(&base_url, Some(&api_token))
///     .expect("Failed to create client");/// 
/// let pid = "doi:10.5072/FK2/QJ8MRH";///
///
/// let response = publish_dataset(&client, &pid, Version::Major).await?;
///
///  println!("Dataset published: {:?}", response);
///
///  # Ok(())
/// }
/// ```
pub async fn publish_dataset(
    client: &BaseClient,
    pid: &str,
    version: Version,
) -> Result<Response<DatasetPublishResponse>, String> {
    // Endpoint metadata
    let url = "/api/datasets/:persistentId/actions/:publish";

    // Determine version
    let version = match version {
        Version::Major => "major".to_string(),
        Version::Minor => "minor".to_string(),
        Version::UpdateCurrent => "updateCurrent".to_string(),
    };

    // Build request parameters
    let parameters = Some(HashMap::from([
        ("persistentId".to_string(), pid.to_owned()),
        ("type".to_string(), version.to_owned()),
    ]));

    // Send request
    let context = RequestType::Plain;
    let response = client.post(url, parameters, context, None).await;

    evaluate_response::<DatasetPublishResponse>(response).await
}

#[cfg(test)]
mod tests {
    use crate::prelude::{BaseClient, dataset};
    use crate::test_utils;

    /// Tests the major version publishing of a dataset.
    ///
    /// This test ensures that a dataset can be successfully published with a major version update.
    /// It sets up a client using API token and base URL obtained from environment variables, creates
    /// a dataset under a specified parent dataverse ("Root"), and then publishes the dataset with a
    /// major version update. The test asserts that the publishing operation was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the publishing
    /// operation fails, indicating an issue with the dataset publishing process.
    #[tokio::test]
    async fn test_publish_dataset() {
        // Set up the client
        let (api_token, base_url, _) = test_utils::extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = test_utils::create_test_dataset(&client, "Root").await;

        // Publish the dataset
        let response = dataset::publish::publish_dataset(
            &client,
            &pid,
            dataset::publish::Version::Major,
        ).await;

        // Assert that the dataset was successfully published
        assert!(response.is_ok());
    }

    /// Tests the minor version publishing of a dataset.
    ///
    /// This test verifies that a dataset can be successfully published with a minor version update.
    /// It follows the same setup as the previous test but specifies a minor version update for the
    /// publishing operation. The test asserts that the publishing operation was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the publishing
    /// operation fails, indicating an issue with the dataset publishing process.
    #[tokio::test]
    async fn test_publish_dataset_minor() {
        // Set up the client
        let (api_token, base_url, _) = test_utils::extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = test_utils::create_test_dataset(&client, "Root").await;

        // Publish the dataset
        let response = dataset::publish::publish_dataset(
            &client,
            &pid,
            dataset::publish::Version::Minor,
        ).await;

        // Assert that the dataset was successfully published
        assert!(response.is_ok());
    }

    /// Tests the update current version publishing of a dataset.
    ///
    /// This test ensures that a dataset can be successfully published with an update to the current version.
    /// It sets up a client using API token and base URL obtained from environment variables, creates
    /// a dataset under a specified parent dataverse ("Root"), and then publishes the dataset with an
    /// update to the current version. The test asserts that the publishing operation was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the publishing
    /// operation fails, indicating an issue with the dataset publishing process.
    #[tokio::test]
    async fn test_publish_dataset_update_current() {
        // Set up the client
        let (api_token, base_url, _) = test_utils::extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = test_utils::create_test_dataset(&client, "Root").await;

        // Publish the dataset
        let response = dataset::publish::publish_dataset(
            &client,
            &pid,
            dataset::publish::Version::UpdateCurrent,
        ).await;

        // Assert that the dataset was successfully published
        assert!(response.is_ok());
    }

    /// Tests publishing a dataset with a non-existent persistent identifier (PID).
    ///
    /// This test attempts to publish a dataset using a non-existent PID to verify that the API
    /// correctly handles such requests by returning an error. It sets up a client using API token
    /// and base URL obtained from environment variables and attempts to publish a dataset
    /// with a known non-existent PID. The test asserts that the publishing request fails as expected.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the client fails to be created, indicating an issue with the
    /// environment variables or the API connectivity. It will also panic if the publishing request
    /// does not fail as expected, indicating an issue with error handling for non-existent PIDs.
    #[tokio::test]
    async fn test_publish_dataset_non_existent_pid() {
        // Set up the client
        let (api_token, base_url, _) = test_utils::extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Publish the dataset
        let response = dataset::publish::publish_dataset(
            &client,
            "non-existent-pid",
            dataset::publish::Version::Major,
        ).await;

        // Assert that the dataset was not published
        let message = serde_json::to_string(&response).unwrap();
        assert!(message.contains("ERROR")); // Ugly, but it works
    }

    /// Tests the `Version` enum's ability to be parsed from string literals.
    ///
    /// This function tests the parsing functionality of the `Version` enum by attempting to parse
    /// various string literals into `Version` variants. It verifies that valid version strings
    /// ("major", "minor", "updatecurrent") are correctly parsed into their corresponding enum variants,
    /// and asserts that an invalid version string results in an error.
    ///
    /// # Assertions
    /// - Asserts that "major" is parsed as `Version::Major`.
    /// - Asserts that "minor" is parsed as `Version::Minor`.
    /// - Asserts that "updatecurrent" is parsed as `Version::UpdateCurrent`.
    /// - Asserts that an invalid version string like "invalid" results in a parsing error.
    #[test]
    fn test_version_from_str() {
        let major = "major".parse::<dataset::publish::Version>();
        assert!(major.is_ok());
        assert_eq!(major.unwrap(), dataset::publish::Version::Major);

        let minor = "minor".parse::<dataset::publish::Version>();
        assert!(minor.is_ok());
        assert_eq!(minor.unwrap(), dataset::publish::Version::Minor);

        let update_current = "updatecurrent".parse::<dataset::publish::Version>();
        assert!(update_current.is_ok());
        assert_eq!(update_current.unwrap(), dataset::publish::Version::UpdateCurrent);

        let invalid = "invalid".parse::<dataset::publish::Version>();
        assert!(invalid.is_err());
    }
}