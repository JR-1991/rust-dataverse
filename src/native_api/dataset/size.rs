use typify::import_types;

use crate::client::{evaluate_response, BaseClient};
use crate::datasetversion::{determine_version, DatasetVersion};
use crate::identifier::Identifier;
use crate::request::RequestType;
use crate::response::Response;
use crate::utils::get_dataset_id;

import_types!(schema = "models/dataset/size.json", struct_builder = true,);

/// Retrieves the size of a dataset for a specific version.
///
/// This asynchronous function performs the following steps:
/// 1. Parses the dataset identifier.
/// 2. Determines the version of the dataset.
/// 3. Constructs the API endpoint path.
/// 4. Sends a GET request to the API to retrieve the dataset size.
/// 5. Evaluates the response and returns the dataset size.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`,
///          representing the unique identifier of the dataset.
/// * `version` - An optional `DatasetVersion` specifying the version of the dataset.
///
/// # Returns
///
/// A `Result` wrapping a `Response<DatasetSizeResponse>` indicating the size of the dataset.
/// If an error occurs during the process, it returns a string describing the error.
///
/// # Errors
///
/// This function will return an error if:
/// - The dataset identifier cannot be parsed.
/// - The dataset version cannot be determined.
/// - The API request fails.
/// - The response evaluation fails.
pub async fn get_dataset_size(
    client: &BaseClient,
    id: &Identifier,
    version: &Option<DatasetVersion>,
) -> Result<Response<DatasetSizeResponse>, String> {
    // Parse the identifier
    let id = match id {
        Identifier::Id(id) => *id,
        Identifier::PersistentId(_) => get_dataset_id(client, &id, version.clone()).await?,
    };

    // Parse the version
    let version: String = determine_version(version, client.has_api_token()).to_string();

    // Endpoint metadata
    let path = format!("/api/datasets/{id}/versions/{version}/downloadsize");

    // Perform the request
    let context = RequestType::Plain {};
    let response = client.get(&path, None, context, None).await;

    evaluate_response::<DatasetSizeResponse>(response).await
}
