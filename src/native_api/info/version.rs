use regress;
use typify::import_types;

use crate::{
    client::{BaseClient, evaluate_response},
    request::RequestType,
    response::Response,
};

import_types!(schema = "models/info/version.json");

/// Retrieves the version information of the Dataverse instance.
///
/// This asynchronous function sends a GET request to the API endpoint designated for retrieving version information.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
///
/// # Returns
///
/// A `Result` wrapping a `Response<VersionResponse>`, which contains the HTTP response status and the deserialized
/// response data indicating the version information, if the request is successful, or a `String` error message on failure.
pub async fn get_version(client: &BaseClient) -> Result<Response<VersionResponse>, String> {
    let context = RequestType::Plain;
    let response = client.get("api/info/version", None, context, None).await;

    evaluate_response::<VersionResponse>(response).await
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;

    use super::*;

    lazy_static! {
        static ref BASE_URL: String = std::env::var("BASE_URL")
            .expect("BASE_URL must be set for tests");
        static ref DV_VERSION: String = std::env::var("DV_VERSION")
            .expect("DV_VERSION must be set for tests");
    }

    #[tokio::test]
    async fn test_get_version() {
        // Arrange
        let client = BaseClient::new(&BASE_URL, None).unwrap();

        // Act
        let response = get_version(&client)
            .await
            .expect("Could not get version");

        // Assert
        assert_eq!(
            response.data.unwrap().version.to_string(),
            *VersionResponseVersion(DV_VERSION.to_string())
        );
    }
}
