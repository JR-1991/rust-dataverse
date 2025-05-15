use typify::import_types;

use crate::{
    client::{evaluate_response, BaseClient},
    request::RequestType,
    response::Response,
};

import_types!(schema = "models/admin/collection/storage-drivers.json");

/// Gets a list of all available storage drivers in the Dataverse installation.
///
/// # Arguments
/// * `client` - The base client used to make the request
///
/// # Returns
/// A Result containing either:
/// * A Response with StorageDriversResponse on success
/// * A String error message on failure
pub async fn get_storage_drivers(
    client: &BaseClient,
) -> Result<Response<StorageDriversResponse>, String> {
    // Endpoint metadata
    let url = "api/admin/dataverse/storageDrivers".to_string();

    // Send request
    let context = RequestType::Plain;
    let response = client.get(url.as_str(), None, context, None).await;

    evaluate_response::<StorageDriversResponse>(response).await
}

/// Sets the storage driver for a specific collection.
///
/// # Arguments
/// * `client` - The base client used to make the request
/// * `alias` - The alias of the collection to modify
/// * `driver` - The storage driver to set for the collection
///
/// # Returns
/// A Result containing either:
/// * A Response with SetStorageDriverResponse on success
/// * A String error message on failure
pub async fn set_collection_storage_driver(
    client: &BaseClient,
    alias: &str,
    driver: &str,
) -> Result<Response<SetStorageDriverResponse>, String> {
    let url = format!("api/admin/dataverse/{}/storageDriver", alias);

    let context = RequestType::Raw(driver.to_string());
    let response = client.put(url.as_str(), None, context, None).await;

    evaluate_response::<SetStorageDriverResponse>(response).await
}

/// Gets the storage driver for a specific collection.
///
/// # Arguments
/// * `client` - The base client used to make the request
/// * `alias` - The alias of the collection to get the storage driver for
///
/// # Returns
/// A Result containing either:
/// * A Response with GetStorageDriverResponse on success
/// * A String error message on failure
pub async fn get_collection_storage_driver(
    client: &BaseClient,
    alias: &str,
) -> Result<Response<GetStorageDriverResponse>, String> {
    let url = format!("api/admin/dataverse/{}/storageDriver", alias);
    let response = client
        .get(url.as_str(), None, RequestType::Plain, None)
        .await;
    evaluate_response::<GetStorageDriverResponse>(response).await
}

/// Resets the storage driver for a specific collection to the default driver.
///
/// # Arguments
/// * `client` - The base client used to make the request
/// * `alias` - The alias of the collection to reset the storage driver for
///
/// # Returns
/// A Result containing either:
/// * A Response with ResetStorageDriverResponse on success
/// * A String error message on failure
pub async fn reset_collection_storage_driver(
    client: &BaseClient,
    alias: &str,
) -> Result<Response<ResetStorageDriverResponse>, String> {
    let url = format!("/api/admin/dataverse/{}/storageDriver", alias);
    let response = client
        .delete(url.as_str(), None, RequestType::Plain, None)
        .await;
    evaluate_response::<ResetStorageDriverResponse>(response).await
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{create_test_client, create_test_collection};

    use super::*;

    #[tokio::test]
    async fn test_set_collection_storage_driver() {
        // ARRANGE
        let client = create_test_client();
        let collection_alias = create_test_collection(&client, "Root").await;

        // ACT
        let response =
            set_collection_storage_driver(&client, &collection_alias, "LocalStack").await;

        // ASSERT
        assert!(response.is_ok());
        let response = get_collection_storage_driver(&client, &collection_alias).await;
        assert!(response.is_ok());
        let storage_driver = response
            .expect("Failed to get storage driver")
            .data
            .expect("Failed to get storage driver data")
            .message
            .expect("Failed to get storage driver message");

        assert_eq!(storage_driver, "localstack1");
    }

    #[tokio::test]
    async fn test_get_storage_drivers() {
        // ARRANGE
        let client = create_test_client();

        // ACT
        let response = get_storage_drivers(&client).await;

        // ASSERT
        assert!(response.is_ok());

        let storage_drivers = response
            .expect("Failed to get storage drivers")
            .data
            .expect("Failed to get storage drivers data");

        assert!(storage_drivers.len() > 0);
    }
}
