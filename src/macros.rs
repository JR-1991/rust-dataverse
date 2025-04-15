/// Checks if a dataset has any locks and waits until they are released.
///
/// This macro polls the Dataverse API to check for locks on a dataset. If locks exist,
/// it will wait and retry until all locks are released. This is useful for operations
/// that require exclusive access to a dataset.
///
/// # Arguments
///
/// * `$client` - A reference to a BaseClient instance used to make API requests
/// * `$id` - An Identifier for the dataset to check locks on
///
/// # Returns
///
/// * `true` - If all locks were successfully released
/// * `false` - If there was an error checking for locks
#[macro_export]
macro_rules! check_lock {
    ($client:expr, $id:expr) => {{
        use crate::native_api::dataset::locks::get_locks;

        // From then env, get the timeout (in seconds)
        // We default to 20 seconds, but this can be overridden
        // by setting the DATAVERSE_LOCK_TIMEOUT environment variable
        let timeout = std::env::var("DATAVERSE_LOCK_TIMEOUT")
            .unwrap_or("20".to_string())
            .parse::<u64>()
            .unwrap_or(20)
            * 1000; // Convert to milliseconds

        let time = std::time::Instant::now();

        while time.elapsed() < std::time::Duration::from_millis(timeout) {
            let response = get_locks($client, $id, None).await;
            if let Ok(lock_response) = response {
                if let Some(data) = lock_response.data {
                    if data.is_empty() {
                        break;
                    }
                    // Wait a moment before checking again
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                } else {
                    break; // No data means no locks
                }
            }
        }
    }};
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        client::BaseClient,
        prelude::{
            dataset::{
                get_dataset_meta, get_locks, locks::LockType, remove_lock, set_lock,
                upload_file_to_dataset,
            },
            Identifier,
        },
        test_utils::{create_test_collection, create_test_dataset, extract_test_env},
    };

    #[tokio::test]
    async fn test_check_lock() {
        // Arrange
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");
        let collection_alias = create_test_collection(&client, "Root").await;
        let (_, pid) = create_test_dataset(&client, &collection_alias).await;

        // Act
        // 1) Set a lock on the dataset
        set_lock(
            &client,
            &Identifier::PersistentId(pid.clone()),
            LockType::Ingest,
        )
        .await
        .expect("Failed to set lock");

        // 2) Spawn two processes:
        //    - One that will delay the unlock
        //    - One that will use the check_lock macro to wait for the lock to be released
        tokio::join!(
            async {
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                remove_lock(
                    &client,
                    &Identifier::PersistentId(pid.clone()),
                    Some(LockType::Ingest),
                )
                .await
                .expect("Failed to remove lock");
            },
            async {
                check_lock!(&client, &Identifier::PersistentId(pid.clone()));
                let fpath = PathBuf::from("tests/fixtures/file.txt");
                upload_file_to_dataset(
                    &client,
                    Identifier::PersistentId(pid.clone()),
                    fpath,
                    None,
                    None,
                )
                .await
                .expect("Failed to upload file");
            },
        );

        // Assert
        let response = get_locks(&client, &Identifier::PersistentId(pid.clone()), None)
            .await
            .expect("Failed to get locks");

        if let Some(locks) = response.data {
            assert!(locks.is_empty());
        } else {
            panic!("Lock request failed");
        }

        let metadata = get_dataset_meta(&client, &Identifier::PersistentId(pid.clone()), &None)
            .await
            .expect("Failed to get metadata");

        assert!(metadata.status.is_ok());
        if let Some(metadata) = metadata.data {
            assert!(metadata.files.len() > 0);
        } else {
            panic!("Metadata request failed");
        }
    }

    #[tokio::test]
    async fn test_check_lock_exceeds_timeout() {
        // Arrange
        std::env::set_var("DATAVERSE_LOCK_TIMEOUT", "4");
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");

        // Create a test dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        set_lock(
            &client,
            &Identifier::PersistentId(pid.clone()),
            LockType::Ingest,
        )
        .await
        .expect("Failed to set lock");

        // Act
        let time = std::time::Instant::now();
        check_lock!(&client, &Identifier::PersistentId(pid.clone()));

        // Assert
        assert!(time.elapsed() > std::time::Duration::from_secs(4));

        let locks = get_locks(&client, &Identifier::PersistentId(pid.clone()), None)
            .await
            .expect("Failed to get locks");

        assert!(locks.status.is_ok());
        assert!(locks.data.is_some());

        // Lock should still be set
        if let Some(locks) = locks.data {
            assert!(locks.len() == 1);
            assert_eq!(locks[0].lock_type, LockType::Ingest);
        } else {
            panic!("Lock request failed");
        }
    }
}
