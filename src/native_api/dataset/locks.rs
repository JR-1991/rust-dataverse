use std::collections::HashMap;

use typify::import_types;

use crate::{
    client::{evaluate_response, BaseClient},
    prelude::Identifier,
    request::RequestType,
    response::Response,
};

import_types!(schema = "models/dataset/locks.json", struct_builder = true,);

/// Retrieves locks for a dataset.
///
/// This asynchronous function fetches the locks associated with a dataset identified by the provided ID.
/// It can optionally filter the locks by a specific lock type.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`, representing the unique identifier of the dataset.
/// * `lock_type` - An optional `LockType` enum value to filter locks by type.
///
/// # Returns
///
/// A `Result` wrapping a `Response<LockResponse>`, which contains the HTTP response status and the deserialized
/// response data with information about the locks, if the request is successful, or a `String` error message on failure.
///
/// # Errors
///
/// This function will return an error if:
/// - The request to the API fails.
/// - The response cannot be properly deserialized.
pub async fn get_locks(
    client: &BaseClient,
    id: &Identifier,
    lock_type: Option<LockType>,
) -> Result<Response<LockResponse>, String> {
    // Endpoint metadata
    let path = match id {
        Identifier::PersistentId(_) => "api/datasets/:persistentId/locks".to_string(),
        Identifier::Id(id) => format!("api/datasets/{id}/locks"),
    };

    // Build parameters
    let mut parameters = HashMap::new();
    if let Some(lock_type) = lock_type {
        parameters.insert("type".to_string(), lock_type.to_string());
    }

    // If it's a PersistentId, add it to the parameters
    if let Identifier::PersistentId(pid) = &id {
        parameters.insert("persistentId".to_string(), pid.clone());
    }

    // Send request
    let context = RequestType::Plain;
    let response = client
        .get(path.as_str(), Some(parameters), context, None)
        .await;

    evaluate_response::<LockResponse>(response).await
}

/// Sets a lock on a dataset.
///
/// This function applies a specific lock type to a dataset, which can prevent certain operations
/// from being performed on the dataset until the lock is released.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`, representing the unique identifier of the dataset.
/// * `lock_type` - A `LockType` enum value specifying the type of lock to apply.
///
/// # Returns
///
/// A `Result` wrapping a `Response<SetLockResponse>`, which contains the HTTP response status and the deserialized
/// response data with information about the lock operation, if the request is successful, or a `String` error message on failure.
///
/// # Errors
///
/// This function will return an error if:
/// - The request to the API fails.
/// - The response cannot be properly deserialized.
pub async fn set_lock(
    client: &BaseClient,
    id: &Identifier,
    lock_type: LockType,
) -> Result<Response<SetLockResponse>, String> {
    // Endpoint metadata
    let path = match id {
        Identifier::PersistentId(pid) => {
            format!(
                "api/datasets/:persistentId/lock/{lock_type}?persistentId={pid}"
            )
        }
        Identifier::Id(id) => format!("api/datasets/{id}/lock/{lock_type}"),
    };

    // Send request
    let context = RequestType::Plain;
    let response = client.post(path.as_str(), None, context, None).await;

    evaluate_response::<SetLockResponse>(response).await
}

/// Removes a lock from a dataset.
///
/// This function removes a specific lock type from a dataset, allowing the dataset to be modified again.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`, representing the unique identifier of the dataset.
/// * `lock_type` - An optional `LockType` enum value specifying the type of lock to remove. If set to `None`, all locks will be removed.
///
/// # Returns
///
/// A `Result` wrapping a `Response<SetLockResponse>`, which contains the HTTP response status and the deserialized
/// response data with information about the lock operation, if the request is successful, or a `String` error message on failure.
///
/// # Errors
///
/// This function will return an error if:
/// - The request to the API fails.
pub async fn remove_lock(
    client: &BaseClient,
    id: &Identifier,
    lock_type: Option<LockType>,
) -> Result<Response<SetLockResponse>, String> {
    // Endpoint metadata
    let path = match &id {
        Identifier::PersistentId(_) => "api/datasets/:persistentId/locks".to_string(),
        Identifier::Id(id) => format!("api/datasets/{id}/locks"),
    };

    // Build parameters
    let mut parameters = HashMap::new();
    if let Some(lock_type) = lock_type {
        parameters.insert("type".to_string(), lock_type.to_string());
    }

    // If it's a PersistentId, add it to the parameters
    if let Identifier::PersistentId(pid) = &id {
        parameters.insert("persistentId".to_string(), pid.clone());
    }

    // Send request
    let context = RequestType::Plain;
    let response = client
        .delete(path.as_str(), Some(parameters), context, None)
        .await;

    evaluate_response::<SetLockResponse>(response).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        prelude::{dataset::submit_for_review, *},
        response::Status,
        test_utils::{create_test_dataset, extract_test_env},
    };

    #[tokio::test]
    async fn test_empty_locks() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Check if the locks are empty
        let locks = get_locks(&client, &Identifier::PersistentId(pid), None)
            .await
            .expect("Failed to get locks");

        assert_eq!(locks.status, Status::OK);
        assert!(locks.data.is_some());
    }

    #[tokio::test]
    async fn test_review_lock() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Submit for review
        submit_for_review(&client, &Identifier::PersistentId(pid.clone()))
            .await
            .expect("Failed to submit for review");

        // Get locks
        let locks = get_locks(&client, &Identifier::PersistentId(pid.clone()), None)
            .await
            .expect("Failed to get locks");

        assert_eq!(locks.status, Status::OK);
        assert!(locks.data.is_some());

        if let Some(locks) = locks.data {
            assert_eq!(locks.len(), 1);
            assert_eq!(locks[0].lock_type, LockType::InReview);
        } else {
            panic!("No locks found");
        }
    }

    #[tokio::test]
    async fn test_review_lock_and_filter_by_different_lock_type() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Submit for review
        submit_for_review(&client, &Identifier::PersistentId(pid.clone()))
            .await
            .expect("Failed to submit for review");

        // Get locks
        let locks = get_locks(
            &client,
            &Identifier::PersistentId(pid.clone()),
            Some(LockType::Ingest),
        )
        .await
        .expect("Failed to get locks");

        assert_eq!(locks.status, Status::OK);
        assert!(locks.data.is_some());
        let locks = locks.data.unwrap();
        println!("{locks:?}");
        assert_eq!(locks.len(), 0);
    }

    #[tokio::test]
    async fn test_set_lock() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Set the lock
        let response = set_lock(
            &client,
            &Identifier::PersistentId(pid.clone()),
            LockType::Ingest,
        )
        .await
        .expect("Failed to set lock");

        if let Status::ERROR = response.status {
            panic!("Failed to set lock: {response:?}");
        }

        // Get locks
        let locks = get_locks(&client, &Identifier::PersistentId(pid.clone()), None)
            .await
            .expect("Failed to get locks");

        if let Some(locks) = locks.data {
            assert_eq!(locks.len(), 1);
            assert_eq!(locks[0].lock_type, LockType::Ingest);
        } else {
            panic!("No locks found");
        }
    }

    #[tokio::test]
    async fn test_remove_all_locks() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (id, _) = create_test_dataset(&client, "Root").await;

        // Set the lock
        set_lock(&client, &Identifier::Id(id), LockType::Ingest)
            .await
            .expect("Failed to set lock");

        // Remove the lock
        remove_lock(&client, &Identifier::Id(id), None)
            .await
            .expect("Failed to remove lock");

        // Get locks
        let locks = get_locks(&client, &Identifier::Id(id), None)
            .await
            .expect("Failed to get locks");

        assert_eq!(locks.status, Status::OK);

        if let Some(locks) = locks.data {
            assert_eq!(locks.len(), 0);
        } else {
            panic!("No locks found");
        }
    }

    #[tokio::test]
    async fn test_remove_lock_by_type() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (id, _) = create_test_dataset(&client, "Root").await;

        // Set the lock
        set_lock(&client, &Identifier::Id(id), LockType::Workflow)
            .await
            .expect("Failed to set lock");

        // Set the lock
        set_lock(&client, &Identifier::Id(id), LockType::InReview)
            .await
            .expect("Failed to set lock");

        // Remove the lock
        remove_lock(&client, &Identifier::Id(id), Some(LockType::InReview))
            .await
            .expect("Failed to remove lock");

        // Get locks
        let locks = get_locks(&client, &Identifier::Id(id), None)
            .await
            .expect("Failed to get locks");

        assert_eq!(locks.status, Status::OK);

        if let Some(locks) = locks.data {
            assert_eq!(locks.len(), 1);
            assert_eq!(locks[0].lock_type, LockType::Workflow);
        } else {
            panic!("No locks found");
        }
    }
}
