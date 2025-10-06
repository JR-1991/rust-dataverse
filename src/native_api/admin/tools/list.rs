//! External tool listing functionality.
//!
//! This module provides functionality for listing external tools registered with a Dataverse installation.
//! External tools can operate at the file or dataset level and provide various capabilities like
//! explore, preview, query, and configure operations.

use crate::{
    client::{evaluate_response, BaseClient},
    native_api::admin::tools::add::ExternalToolResponse,
    request::RequestType,
    response::Response,
};

/// Lists all external tools registered with the Dataverse installation.
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
///
/// # Returns
///
/// Returns a `Result` containing:
/// - `Ok(Response<Vec<ExternalToolResponse>>)` - Success response with a list of all registered tools
/// - `Err(String)` - Error message if the listing fails
pub async fn list_external_tools(
    client: &BaseClient,
) -> Result<Response<Vec<ExternalToolResponse>>, String> {
    // Endpoint metadata
    let url = "api/admin/externalTools".to_string();

    // Send request
    let context = RequestType::Plain;
    let response = client.get(url.as_str(), None, context, None).await;

    evaluate_response::<Vec<ExternalToolResponse>>(response).await
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;

    use crate::native_api::admin::tools;

    use super::*;

    lazy_static! {
        static ref BASE_URL: String =
            std::env::var("BASE_URL").expect("BASE_URL must be set for tests");
        static ref DV_VERSION: String =
            std::env::var("DV_VERSION").expect("DV_VERSION must be set for tests");
    }

    #[tokio::test]
    async fn test_list_external_tools() {
        // Arrange
        let client = BaseClient::new(&BASE_URL, None).unwrap();

        // Add a tool first
        let manifest_content =
            std::fs::read_to_string("tests/fixtures/external_tool_test.json").unwrap();
        let manifest: tools::manifest::ExternalToolManifest =
            serde_json::from_str(&manifest_content).unwrap();
        let response = tools::add::register_external_tool(&client, manifest, true).await;
        assert!(response.is_ok());

        // Act
        let response = list_external_tools(&client)
            .await
            .expect("Could not list external tools");

        // Assert
        let tools = response.data.clone().unwrap();
        assert!(!tools.is_empty());

        // Check if there's a tool with the name "fabulous"
        let has_fabulous_tool = tools
            .iter()
            .any(|tool| tool.tool_name.as_ref().unwrap() == "fabulous");
        assert!(
            has_fabulous_tool,
            "Expected to find a tool named 'fabulous'"
        );
    }
}
