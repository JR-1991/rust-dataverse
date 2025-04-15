use crate::{
    client::BaseClient,
    prelude::Identifier,
    request::RequestType,
    response::{Response, Status},
};

/// Exports a dataset in a specified format.
///
/// This asynchronous function exports a dataset, identified by either a persistent identifier or a numeric ID,
/// using the specified exporter format. It constructs the appropriate API endpoint URL based on the identifier type,
/// sends a GET request to perform the export operation, and returns the exported content as a string.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`,
///          representing the unique identifier of the dataset to be exported.
/// * `exporter` - A string slice that specifies the export format to use (e.g., "ddi", "oai_ddi", "datacite", etc.).
///
/// # Returns
///
/// A `Result` wrapping a `Response<String>`, which contains the HTTP response status and the exported dataset content
/// as a string if the request is successful, or a `String` error message on failure.
/// ```
pub async fn export_dataset(
    client: &BaseClient,
    id: &Identifier,
    exporter: &str,
) -> Result<Response<String>, String> {
    // Parse the identifier
    let path = match id {
        Identifier::Id(id) => format!("/api/datasets/{id}/export?exporter={exporter}"),
        Identifier::PersistentId(id) => {
            format!("/api/datasets/export?exporter={exporter}&persistentId={id}")
        }
    };

    // Perform the request
    let context = RequestType::Plain {};
    let response = client.get(&path, None, context, None).await;

    let export_response = response
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())?;

    // Return either parsed JSON response or raw string content
    serde_json::from_str::<Response<String>>(&export_response)
        .or_else(|_| Ok(Response::<String>::from_string(export_response, Status::OK)))
}
