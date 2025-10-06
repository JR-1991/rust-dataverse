//! External tool registration functionality.
//!
//! This module provides functionality for registering external tools with a Dataverse installation.
//! External tools can operate at the file or dataset level and provide various capabilities like
//! explore, preview, query, and configure operations.

use serde_json;
use typify::import_types;

use crate::{
    client::{evaluate_response, BaseClient},
    request::RequestType,
    response::Response,
};

use super::manifest::ExternalToolManifest;

import_types!(
    schema = "models/admin/tools/tool-response.json",
    struct_builder = true,
);

/// Registers an external tool with the Dataverse installation.
///
/// External tools extend the functionality of Dataverse by providing additional
/// capabilities for working with files and datasets. Tools can be of various types:
/// - **Explore**: Tools that help users explore data
/// - **Preview**: Tools that provide preview functionality for files
/// - **Query**: Tools that allow querying data
/// - **Configure**: Tools that provide configuration options
///
/// # Arguments
///
/// * `client` - The authenticated client for making API requests
/// * `body` - The external tool manifest containing tool configuration
///
/// # Returns
///
/// Returns a `Result` containing:
/// - `Ok(Response<ExternalToolResponse>)` - Success response with the registered tool details
/// - `Err(String)` - Error message if the registration fails
pub async fn register_external_tool(
    client: &BaseClient,
    body: ExternalToolManifest,
) -> Result<Response<ExternalToolResponse>, String> {
    // Endpoint metadata
    let url = "api/admin/externalTools".to_string();

    // Build body
    let body = serde_json::to_string(&body).unwrap();

    // Send request
    let context = RequestType::JSON { body: body.clone() };
    let response = client.post(url.as_str(), None, context, None).await;

    evaluate_response::<ExternalToolResponse>(response).await
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;

    use super::*;

    lazy_static! {
        static ref BASE_URL: String =
            std::env::var("BASE_URL").expect("BASE_URL must be set for tests");
    }

    #[tokio::test]
    async fn test_register_external_tool() {
        // Arrange
        let client = BaseClient::new(&BASE_URL, None).unwrap();

        // Act
        let manifest_content =
            std::fs::read_to_string("tests/fixtures/external_tool_test.json").unwrap();
        let mut manifest: ExternalToolManifest = serde_json::from_str(&manifest_content).unwrap();
        let random_suffix = rand::random::<u16>() % 1001;
        manifest.tool_name = Some(format!("fabulous{random_suffix}"));
        let response = register_external_tool(&client, manifest.clone()).await;

        // Assert
        assert!(response.is_ok());
    }
}
