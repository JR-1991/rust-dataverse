//! External tool removal functionality.
//!
//! This module provides functionality for removing external tools from a Dataverse installation.
//! External tools can operate at the file or dataset level and provide various capabilities like
//! explore, preview, query, and configure operations.

use typify::import_types;

use crate::{
    client::{evaluate_response, BaseClient},
    request::RequestType,
    response::{Message, Response},
};

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
/// * `tool_id` - The ID of the external tool to remove
///
/// # Returns
///
/// Returns a `Result` containing:
/// - `Ok(Response<ExternalToolResponse>)` - Success response with the registered tool details
/// - `Err(String)` - Error message if the registration fails
pub async fn remove_external_tool(
    client: &BaseClient,
    tool_id: i64,
) -> Result<Response<Message>, String> {
    // Endpoint metadata
    let url = format!("api/admin/externalTools/{}", tool_id);

    // Send request
    let context = RequestType::Plain;
    let response = client.delete(url.as_str(), None, context, None).await;

    evaluate_response::<Message>(response).await
}

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;

    use crate::native_api::admin::tools::{
        add::register_external_tool, manifest::ExternalToolManifest,
    };

    use super::*;

    lazy_static! {
        static ref BASE_URL: String =
            std::env::var("BASE_URL").expect("BASE_URL must be set for tests");
    }

    #[tokio::test]
    async fn test_remove_external_tool() {
        // Arrange
        let client = BaseClient::new(&BASE_URL, None).unwrap();

        // Act
        let manifest_content =
            std::fs::read_to_string("tests/fixtures/external_tool_test.json").unwrap();
        let mut manifest: ExternalToolManifest = serde_json::from_str(&manifest_content).unwrap();

        let random_suffix = rand::random::<u16>() % 1001;
        manifest.tool_name = format!("fabulous{random_suffix}");

        let added = register_external_tool(&client, manifest.clone(), false)
            .await
            .expect("Could not register external tool")
            .data
            .unwrap();

        let tool_id = added.id;
        let response = remove_external_tool(&client, tool_id)
            .await
            .expect("Could not remove external tool: Request failed");

        // Assert
        assert!(response.status.is_ok());
    }

    #[should_panic]
    #[tokio::test]
    async fn test_remove_external_tool_not_found() {
        // Arrange
        let client = BaseClient::new(&BASE_URL, None).unwrap();

        // Act
        remove_external_tool(&client, 1234567890)
            .await
            .expect_err("Could not remove external tool: Request failed");
    }
}
