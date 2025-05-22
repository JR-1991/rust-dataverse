use typify::import_types;

use crate::{
    client::{evaluate_response, BaseClient},
    request::RequestType,
    response::Response,
};

import_types!(schema = "models/info/exporters.json");

/// Retrieves the exporters information of the Dataverse instance.
///
/// This asynchronous function sends a GET request to the API endpoint designated for retrieving exporters information.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
///
/// # Returns
///
/// A `Result` wrapping a `Response<ExportersResponse>`, which contains the HTTP response status and the deserialized
/// response data indicating the exporters information, if the request is successful, or a `String` error message on failure.
pub async fn get_exporters(client: &BaseClient) -> Result<Response<ExportersResponse>, String> {
    let context = RequestType::Plain;
    let response = client
        .get("api/info/exportFormats", None, context, None)
        .await;

    evaluate_response::<ExportersResponse>(response).await
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;

    use super::*;

    lazy_static! {
        static ref BASE_URL: String =
            std::env::var("BASE_URL").expect("BASE_URL must be set for tests");
        static ref DV_VERSION: String =
            std::env::var("DV_VERSION").expect("DV_VERSION must be set for tests");
    }

    #[tokio::test]
    async fn test_get_exporters() {
        // Arrange
        let client = BaseClient::new(&BASE_URL, None).unwrap();

        // Act
        let response = get_exporters(&client)
            .await
            .expect("Could not get exporters");

        // Assert
        assert!(response.status.is_ok());
    }
}
