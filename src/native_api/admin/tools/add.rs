//! External tool registration functionality.
//!
//! This module provides functionality for registering external tools with a Dataverse installation.
//! External tools can operate at the file or dataset level and provide various capabilities like
//! explore, preview, query, and configure operations.

use serde_json;
use typify::import_types;

use crate::{
    client::{evaluate_response, BaseClient},
    native_api::admin::tools::{list::list_external_tools, remove::remove_external_tool},
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
/// * `overwrite` - Whether to overwrite an existing tool when it already exists. This will delete the existing tool and register a new one.
///
/// # Returns
///
/// Returns a `Result` containing:
/// - `Ok(Response<ExternalToolResponse>)` - Success response with the registered tool details
/// - `Err(String)` - Error message if the registration fails
pub async fn register_external_tool(
    client: &BaseClient,
    body: ExternalToolManifest,
    overwrite: bool,
) -> Result<Response<ExternalToolResponse>, String> {
    // Endpoint metadata
    let url = "api/admin/externalTools".to_string();

    // Extract all duplicates, if there are any, return an error
    if let Ok(existing_tools) = check_if_tool_exists(client, &body.tool_name).await {
        if !overwrite {
            return Err("Tool already exists in Dataverse".to_string());
        }
        remove_duplicates(client, existing_tools).await?;
    }

    // Build body
    let body = serde_json::to_string(&body).unwrap();

    // Send request
    let context = RequestType::JSON { body: body.clone() };
    let response = client.post(url.as_str(), None, context, None).await;

    evaluate_response::<ExternalToolResponse>(response).await
}

/// Checks if an external tool with the specified name already exists in the Dataverse installation.
///
/// This function queries the Dataverse API to retrieve all registered external tools and then
/// filters the results to find any tools that match the provided tool name. This is particularly
/// useful for preventing duplicate tool registrations or for implementing overwrite functionality
/// where existing tools need to be identified before replacement.
///
/// # Arguments
///
/// * `client` - The authenticated BaseClient instance used for making API requests to the Dataverse installation
/// * `tool_name` - The name of the external tool to search for in the registered tools list
///
/// # Returns
///
/// Returns a `Result` containing:
/// - `Ok(Vec<ExternalToolResponse>)` - A vector of matching external tools if any are found with the specified name
/// - `Err(String)` - An error message if no tools are found with the given name, if the API request fails, or if no external tools exist in the Dataverse installation
///
/// # Implementation Details
///
/// The function performs the following operations:
/// 1. Retrieves all external tools from the Dataverse installation using the list_external_tools API
/// 2. Extracts the tool data from the response, returning an error if no tools exist
/// 3. Filters the tools to find exact matches based on the tool name
/// 4. Returns the matching tools or an appropriate error message if none are found
async fn check_if_tool_exists(
    client: &BaseClient,
    tool_name: &str,
) -> Result<Vec<ExternalToolResponse>, String> {
    let external_tools = list_external_tools(client).await?;
    let external_tools = external_tools
        .data
        .ok_or("No external tools found in Dataverse")?;

    let matching_tools: Vec<ExternalToolResponse> = external_tools
        .into_iter()
        .filter(|tool| tool.tool_name.as_deref() == Some(tool_name))
        .collect();

    if matching_tools.is_empty() {
        Err(format!("No tools found with name '{}'", tool_name))
    } else {
        Ok(matching_tools)
    }
}

/// Removes duplicate external tools from the Dataverse installation by deleting existing tool instances.
///
/// This function iterates through a collection of existing external tools and removes each one
/// from the Dataverse installation using the remove_external_tool API endpoint. This operation
/// is typically performed as part of an overwrite workflow where duplicate tools need to be
/// cleared before registering a new version of the same tool. The function ensures that all
/// provided tools are successfully deleted before completing the operation.
///
/// # Arguments
///
/// * `client` - The authenticated BaseClient instance used for making API requests to the Dataverse installation
/// * `existing_tools` - A vector of ExternalToolResponse objects representing the tools that should be removed from the system
///
/// # Returns
///
/// Returns a `Result` containing:
/// - `Ok(())` - Success indication when all specified tools have been successfully removed from the Dataverse installation
/// - `Err(String)` - An error message if any tool deletion fails, including cases where the API request fails or the deletion operation is rejected by the server
///
/// # Implementation Details
///
/// The function performs the following operations for each tool in the provided collection:
/// 1. Calls the remove_external_tool API endpoint using the tool's unique identifier
/// 2. Checks the response status to ensure the deletion was successful
/// 3. Returns an error immediately if any deletion fails, preventing partial cleanup scenarios
/// 4. Continues processing remaining tools only if all previous deletions succeeded
///
/// # Error Handling
///
/// The function implements fail-fast error handling, meaning that if any single tool deletion
/// fails, the entire operation is aborted and an error is returned. This approach ensures
/// consistency and prevents scenarios where only some duplicate tools are removed.
async fn remove_duplicates(
    client: &BaseClient,
    existing_tools: Vec<ExternalToolResponse>,
) -> Result<(), String> {
    for tool in existing_tools.iter() {
        let response = remove_external_tool(client, tool.id).await?;
        if response.status.is_err() {
            return Err("Failed to delete existing tool".to_string());
        }
    }

    Ok(())
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
        manifest.tool_name = format!("fabulous{random_suffix}");
        let response = register_external_tool(&client, manifest.clone(), false).await;

        // Assert
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_register_external_tool_overwrite() {
        // Arrange
        let client = BaseClient::new(&BASE_URL, None).unwrap();

        // Act
        let manifest_content =
            std::fs::read_to_string("tests/fixtures/external_tool_test.json").unwrap();
        let mut manifest: ExternalToolManifest = serde_json::from_str(&manifest_content).unwrap();
        let random_suffix = rand::random::<u16>() % 1001;
        manifest.tool_name = format!("fabulous{random_suffix}");

        // First registration
        let response1 = register_external_tool(&client, manifest.clone(), false).await;
        assert!(response1.is_ok());

        // Second registration with overwrite allowed
        let response2 = register_external_tool(&client, manifest.clone(), true).await;
        println!("response2: {:?}", response2);
        assert!(response2.is_ok());
    }

    #[should_panic]
    #[tokio::test]
    async fn test_register_external_tool_no_overwrite() {
        // Arrange
        let client = BaseClient::new(&BASE_URL, None).unwrap();

        // Act
        let manifest_content =
            std::fs::read_to_string("tests/fixtures/external_tool_test.json").unwrap();
        let mut manifest: ExternalToolManifest = serde_json::from_str(&manifest_content).unwrap();
        let random_suffix = rand::random::<u16>() % 1001;
        manifest.tool_name = format!("fabulous{random_suffix}");

        // First registration
        let response1 = register_external_tool(&client, manifest.clone(), false).await;
        assert!(response1.is_ok());

        // Second registration without overwrite - should panic
        register_external_tool(&client, manifest.clone(), false)
            .await
            .expect("Should panic when trying to register duplicate tool without overwrite");
    }
}
