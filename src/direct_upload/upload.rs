use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{Duration, Utc};
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use tokio::sync::Semaphore;
use typify::import_types;

use crate::check_lock;
use crate::client::{evaluate_response, BaseClient};
use crate::direct_upload::ticket::get_ticket;
use crate::file::uploadfile::UploadFile;
use crate::prelude::dataset::upload::assemble_upload_body;
use crate::prelude::{CallbackFun, Identifier};
use crate::request::RequestType;
use crate::response::Response;

use super::db::create_database;
use super::hasher::{add_checksum, FileHash, Hasher};
use super::model::Upload;
use super::ticket::{MultiPartTicket, SinglePartTicket, TicketResponse};

import_types!(
    schema = "models/direct_upload/upload.json",
    struct_builder = true,
    derives = [Default]
);

/// A batch of direct upload bodies.
///
/// This is a wrapper around a vector of direct upload bodies.
/// It is used to pass a batch of upload bodies to the batch upload function.
///
/// # Fields
///
/// * `body` - The vector of direct upload bodies
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BatchDirectUploadBody(pub Vec<DirectUploadBody>);

impl From<Vec<DirectUploadBody>> for BatchDirectUploadBody {
    /// Convert a vector of direct upload bodies to a batch of direct upload bodies.
    ///
    /// # Arguments
    ///
    /// * `body` - The vector of direct upload bodies
    ///
    /// # Returns
    ///
    /// A `BatchDirectUploadBody`
    fn from(body: Vec<DirectUploadBody>) -> Self {
        BatchDirectUploadBody(body)
    }
}

/// Uploads a file directly to a Dataverse dataset using the direct upload API.
///
/// This function handles the complete direct upload process which includes:
/// 1. Obtaining an upload ticket from the server
/// 2. Uploading the file content to the storage location specified in the ticket
/// 3. Computing file hashes during upload for integrity verification
/// 4. Registering the uploaded file with the dataset
///
/// This function is the main entry point for the direct upload process and is
/// responsible for handling the complete upload lifecycle.
///
/// # Arguments
///
/// * `client` - The base client used for API communication
/// * `id` - The identifier of the dataset to upload the file to
/// * `file` - The file to upload
/// * `hasher` - The hasher to use for the upload
/// * `body` - The body of the upload request
/// * `callbacks` - The callbacks to use for the upload
///
/// # Returns
///
/// A `Result` containing either:
/// * `Response<DirectUploadResponse>` - The server response after successful upload
/// * `String` - An error message if the upload failed
pub async fn direct_upload(
    client: &BaseClient,
    id: Identifier,
    file: impl Into<UploadFile>,
    hasher: impl TryInto<FileHash, Error = String>,
    body: DirectUploadBody,
    callbacks: Option<Vec<CallbackFun>>,
) -> Result<Response<DirectUploadResponse>, String> {
    // Connect to the database or create it if it doesn't exist
    // This database is used to track the upload progress of multi-part uploads
    // and to store the upload metadata to potentially resume the upload later
    // if something goes wrong
    let pool = create_database(Some("./dvcli_direct_upload.db".to_string()))
        .await
        .map_err(|e| e.to_string())?;

    // Cleanup before starting the upload
    cleanup_expired_and_complete_uploads(&pool).await?;

    let hasher = hasher.try_into()?;
    let body = direct_upload_core(client, &id, file, hasher, body, callbacks, &pool).await?;
    let response = register_file(client, id, body).await?;

    // Cleanup after the upload
    cleanup_expired_and_complete_uploads(&pool).await?;

    Ok(response)
}

/// Uploads a batch of files directly to a Dataverse dataset using the direct upload API.
///
/// This function handles the complete direct upload process for a batch of files.
/// It processes each file in the batch individually and accumulates the results.
/// A semaphore is used to limit concurrent uploads to 5 files at a time.
///
/// # Arguments
///
/// * `client` - The base client used for API communication
/// * `id` - The identifier of the dataset to upload the files to
/// * `files` - A vector of files to upload
/// * `hasher` - The hasher to use for the uploads
/// * `body` - A vector of upload metadata for each file
///
/// # Returns
///
/// A `Result` containing either:
/// * `()` - Indicates successful batch upload
/// * `String` - An error message if the upload failed
pub async fn batch_direct_upload(
    client: &BaseClient,
    id: Identifier,
    files: Vec<impl Into<UploadFile>>,
    hasher: impl TryInto<FileHash, Error = String>,
    body: Vec<DirectUploadBody>,
    n_parallel_uploads: impl Into<Option<usize>>,
) -> Result<Response<BatchDirectUploadResponse>, String> {
    // Connect to the database or create it if it doesn't exist
    // This database is used to track the upload progress of multi-part uploads
    // and to store the upload metadata to potentially resume the upload later
    // if something goes wrong
    let pool = create_database(Some("./dvcli_direct_upload.db".to_string()))
        .await
        .map_err(|e| e.to_string())?;

    // Cleanup before starting the upload
    cleanup_expired_and_complete_uploads(&pool).await?;

    // Check if the number of files matches the number of body entries
    if files.len() != body.len() {
        return Err("Number of files does not match number of body entries".to_string());
    }

    // Create a semaphore to limit concurrent uploads to 5
    let semaphore = Arc::new(Semaphore::new(n_parallel_uploads.into().unwrap_or(1)));
    let pool = Arc::new(pool.clone());
    let client = Arc::new(client.clone());
    let arc_id = Arc::new(id.clone());
    let hasher = hasher.try_into()?;

    // Create a vector to hold the join handles
    let mut handles = Vec::new();

    // Iterate through files and bodies together
    for (_, (file, body)) in files.into_iter().zip(body.into_iter()).enumerate() {
        let file = file.into();
        let semaphore_clone = semaphore.clone();
        let pool_clone = pool.clone();
        let client_clone = client.clone();
        let id_clone = arc_id.clone();
        let hasher_clone = hasher.clone();

        // Spawn a task for each file upload
        let handle = tokio::spawn(async move {
            let _permit = semaphore_clone.acquire().await.unwrap();

            // Upload the file
            let updated_body = direct_upload_core(
                &client_clone,
                &id_clone,
                file,
                hasher_clone,
                body,
                None,
                &pool_clone,
            )
            .await?;

            Ok::<_, String>(updated_body)
        });

        handles.push(handle);
    }

    // Wait for all uploads to complete and collect any errors
    let mut errors = Vec::new();
    let mut bodies = Vec::new();
    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(body)) => bodies.push(body),
            Ok(Err(e)) => errors.push(format!("Error uploading file {}: {}", idx + 1, e)),
            Err(e) => errors.push(format!("Task error for file {}: {}", idx + 1, e)),
        }
    }

    if !errors.is_empty() {
        return Err(format!("Batch upload had errors:\n{}", errors.join("\n")));
    }

    // Cleanup the upload
    cleanup_expired_and_complete_uploads(&pool).await?;

    // Register the files
    register_batch_files(&client, &id, BatchDirectUploadBody::from(bodies)).await
}

/// Core logic for the direct upload process.
///
/// We need to separate this from the main function because we need to return
/// the body with the hash and storage identifier to the caller. This is because
/// we either have a single or batch upload, where different function to register
/// the file are used.
///
/// # Arguments
///
/// * `client` - The base client used for API communication
/// * `id` - Dataset identifier (either persistent ID or database ID)
/// * `file` - The file to be uploaded
/// * `hasher` - The hash algorithm to use for file integrity verification
/// * `body` - Upload metadata including file name, description, etc.
/// * `callbacks` - Optional vector of callback functions for progress tracking
/// * `pool` - The database pool
///
/// # Returns
///
/// A `Result` containing either:
/// * `DirectUploadBody` - The body with the hash and storage identifier
/// * `String` - An error message if the upload failed
async fn direct_upload_core(
    client: &BaseClient,
    id: &Identifier,
    file: impl Into<UploadFile>,
    hasher: FileHash,
    body: DirectUploadBody,
    callbacks: Option<Vec<CallbackFun>>,
    pool: &Pool<Sqlite>,
) -> Result<DirectUploadBody, String> {
    let mut body = body;
    let file = file.into();

    // Cleanup expired and complete uploads
    cleanup_expired_and_complete_uploads(&pool).await?;

    if file.file.is_path() {
        // Get the unique hash identifier for the upload to check if it already exists
        // This one should not be confused with the file hash, which is computed during the upload
        // and is used to verify the integrity of the uploaded file.
        let file_hash_id = Upload::create_hash(
            client.base_url().to_string(),
            file.file.path_ref().unwrap().to_string_lossy().to_string(),
            id.to_string(),
            file.size as i64,
            hasher.algorithm(),
        );

        if let Ok(upload) = Upload::get_by_hash(&pool, &file_hash_id).await {
            // In this case, the upload already exists, so we need to upload the remaining parts
            // Please note, this is only applicable for multi-part uploads.
            upload_remaining_parts(client, &pool, &upload, callbacks).await?;
            panic!("Upload already exists");
        }
    }

    // Fetch tickets
    let ticket_response = get_ticket(client, file.size as usize, id.clone())
        .await?
        .data
        .ok_or("No ticket data")?;

    let (file_hash, file_hash_algo) = match ticket_response {
        TicketResponse::SinglePartTicket(ticket) => {
            let file_hash = upload_single_part(client, &ticket, file, &hasher, callbacks).await?;
            body.storage_identifier = Some(ticket.storage_identifier);
            file_hash
        }
        TicketResponse::MultiPartTicket(ticket) => {
            // Currently, only local files are supported for direct upload
            // This is because currently, direct uploads require a known file size
            // and other file sources may not provide this information.
            if file.file.is_not_path() {
                return Err(
                    "Unsupported file source. Only local files are supported for direct upload."
                        .to_string(),
                );
            }

            let file_hash =
                upload_multi_part(client, &pool, &id, &ticket, file, callbacks, &hasher).await?;
            body.storage_identifier = Some(ticket.storage_identifier.clone());
            file_hash
        }
    };

    // Add the hash to the body
    add_checksum(&mut body, &file_hash_algo, file_hash).map_err(|e| e.to_string())?;

    Ok(body)
}

/// Uploads a file in a single request using the provided ticket information.
///
/// This function handles the actual file upload for files that can be uploaded
/// in a single HTTP request. It configures the necessary callbacks for hash
/// computation and progress tracking, then sends the file to the URL specified
/// in the ticket.
///
/// # Arguments
///
/// * `client` - The base client used for API communication
/// * `ticket` - The single-part upload ticket containing the upload URL and other metadata
/// * `file` - The file to be uploaded
/// * `hasher` - The hash algorithm implementation for file integrity verification
/// * `callbacks` - Optional vector of callback functions for progress tracking
///
/// # Returns
///
/// A `Result` containing either:
/// * `()` - Indicates successful upload
/// * `String` - An error message if the upload failed
async fn upload_single_part(
    client: &BaseClient,
    ticket: &SinglePartTicket,
    file: UploadFile,
    hasher: &FileHash,
    callbacks: Option<Vec<CallbackFun>>,
) -> Result<(String, String), String> {
    // Create a vector of callbacks, if provided, else just the hasher
    let mut callback_vec = vec![hasher.to_callback()];
    if let Some(cbs) = callbacks {
        callback_vec.extend(cbs);
    }

    // If TEST_MODE is set to LocalStack, replace localstack with localhost
    // Otherwise the URL will point to the localstack service with the wrong host
    let url = TestMode::from_str(&std::env::var("TEST_MODE").unwrap_or_default())
        .unwrap_or(TestMode::Off)
        .process_url(ticket.url.as_str());

    // Upload the file
    let context = RequestType::File {
        file,
        callbacks: None,
    };

    let response = client
        .put(url.as_str(), None, context, None)
        .await
        .map_err(|e| e.to_string())?;

    if response.status().is_success() {
        Ok((
            hasher.compute().map_err(|e| e.to_string())?,
            hasher.algorithm().to_string(),
        ))
    } else {
        Err(response.text().await.unwrap_or_default())
    }
}

/// Uploads a file in multiple parts using the provided ticket information.
///
/// This function handles the file upload process for large files that need to be
/// split into multiple chunks and uploaded separately. It is designed for handling
/// files that exceed the single-part upload size limit.
///
/// # Arguments
///
/// * `client` - The base client used for API communication
/// * `ticket` - The multi-part upload ticket containing part URLs and other metadata
/// * `file` - The file to be uploaded
/// * `hasher` - The hash algorithm implementation for file integrity verification
/// * `callbacks` - Optional vector of callback functions for progress tracking
///
/// # Returns
///
/// A `Result` containing either:
/// * `()` - Indicates successful upload
/// * `String` - An error message if the upload failed
async fn upload_multi_part(
    client: &BaseClient,
    pool: &Pool<Sqlite>,
    id: &Identifier,
    ticket: &MultiPartTicket,
    file: UploadFile,
    callbacks: Option<Vec<CallbackFun>>,
    hasher: &FileHash,
) -> Result<(String, String), String> {
    // Create the upload in the database
    let upload = Upload::from_ticket(
        &pool,
        ticket,
        client.base_url().as_str(),
        id,
        &PathBuf::from(file.file.path_ref().unwrap()),
        file.size,
        hasher.algorithm(),
    )
    .await
    .map_err(|e| e.to_string())?;

    // Upload the remaining parts
    let hash_task = hash_and_store(&upload, &pool, &hasher, file);
    let upload_task = upload_remaining_parts(client, &pool, &upload, callbacks);

    let (_, _) = tokio::join!(hash_task, upload_task);

    // Refetch the upload to get the file hash
    let upload = Upload::get_by_hash(&pool, &upload.hash)
        .await
        .map_err(|e| e.to_string())?;

    Ok((
        upload.file_hash.ok_or("No file hash found")?,
        upload.file_hash_algo,
    ))
}

/// Uploads the remaining parts of a file.
///
/// This function uploads the parts of a file that have not yet been uploaded.
/// It uses a semaphore to limit the number of concurrent uploads and a database
/// to track the upload progress.
///
/// # Arguments
///
/// * `client` - The base client used for API communication
/// * `pool` - The database pool
/// * `upload` - The upload to upload the parts for
/// * `hasher` - The hasher to use for the upload
/// * `file` - The file to upload
/// * `callbacks` - The callbacks to use for the upload
///
/// # Returns
///
/// A `Result` containing either:
/// * `()` - Indicates successful upload
/// * `String` - An error message if the upload failed
async fn upload_remaining_parts(
    client: &BaseClient,
    pool: &Pool<Sqlite>,
    upload: &Upload,
    callbacks: Option<Vec<CallbackFun>>,
) -> Result<(), String> {
    // Get all open parts
    let open_parts = upload
        .get_parts(&pool, true)
        .await
        .map_err(|e| e.to_string())?;

    // Create a semaphore to limit concurrent uploads to 3
    let semaphore = Arc::new(Semaphore::new(3));
    let pool = Arc::new(pool.clone());
    let client = Arc::new(client.clone());

    // Create a vector to hold the join handles
    let mut handles = Vec::new();

    // Upload each part in parallel with semaphore limiting concurrency
    for part in open_parts {
        let semaphore_clone = semaphore.clone();
        let pool_clone = pool.clone();
        let client_clone = client.clone();
        let callbacks_clone = callbacks.clone();

        // Spawn a task for each part upload
        let handle = tokio::spawn(async move {
            // Acquire a permit from the semaphore (will wait if 3 are already in use)
            let _permit = semaphore_clone.acquire().await.unwrap();

            // Upload the part
            let etag = part.upload_part(&client_clone, callbacks_clone).await?;

            // Update the part as complete in the DB
            part.update_part_as_complete(etag, &pool_clone)
                .await
                .map_err(|e| e.to_string())?;

            Ok::<_, String>(())
        });

        handles.push(handle);
    }

    // Wait for all uploads to complete
    for handle in handles {
        handle.await.map_err(|e| e.to_string())??;
    }

    // Collect the ETags
    let (e_tags, complete_url) = upload
        .collect_etags(&pool)
        .await
        .map_err(|e| e.to_string())?;

    // Complete the upload
    complete_upload(&client, &complete_url, e_tags).await?;

    Ok(())
}

/// Hashes the file and stores the hash in the database.
///
/// This function hashes the file and stores the hash in the database.
///
/// # Arguments
///
/// * `upload` - The upload to store the hash for
async fn hash_and_store(
    upload: &Upload,
    pool: &Pool<Sqlite>,
    hasher: &FileHash,
    file: UploadFile,
) -> Result<(), String> {
    if upload.file_hash.is_some() {
        return Ok(());
    }

    // Hash the file
    hasher.hash(file).await?;
    let hash_string = hasher.compute().map_err(|e| e.to_string())?;

    // Store the file hash in the database
    upload
        .set_file_hash(hash_string, hasher.algorithm(), pool)
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

/// Completes the upload of a file by sending the ETags to the server.
///
/// This function sends the ETags of the uploaded chunks to the server to complete
/// the upload process. It constructs a JSON payload containing the ETags and
/// sends it as a PUT request to the server.
///
/// # Arguments
///
/// * `client` - The base client used for API communication
/// * `complete_url` - The URL to complete the upload
/// * `e_tags` - A map of part numbers to ETags
///
/// # Returns
///
/// A `Result` containing either:
/// * `()` - Indicates successful upload
/// * `String` - An error message if the upload failed
async fn complete_upload(
    client: &BaseClient,
    complete_url: &str,
    e_tags: HashMap<String, String>,
) -> Result<(), String> {
    let context = RequestType::JSON {
        body: serde_json::to_string(&e_tags).map_err(|e| e.to_string())?,
    };
    let response = client
        .put(&complete_url, None, context, None)
        .await
        .map_err(|e| e.to_string())?;

    if response.status().is_success() {
        Ok(())
    } else {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        println!("Complete upload failed ({}): {}", status, text);
        Err(text)
    }
}

/// Registers an uploaded file with the dataset in Dataverse.
///
/// After a file has been successfully uploaded to the storage location, this function
/// notifies the Dataverse server about the upload completion and provides necessary
/// metadata to associate the file with the dataset.
///
/// # Arguments
///
/// * `client` - The base client used for API communication
/// * `id` - Dataset identifier (either persistent ID or database ID)
/// * `body` - Upload metadata including storage identifier, file name, description, etc.
///
/// # Returns
///
/// A `Result` containing either:
/// * `Response<DirectUploadResponse>` - The server response after successful registration
/// * `String` - An error message if the registration failed
async fn register_file(
    client: &BaseClient,
    id: Identifier,
    body: DirectUploadBody,
) -> Result<Response<DirectUploadResponse>, String> {
    // Check if the dataset has any locks and wait until they are released
    check_lock!(client, &id);

    // Endpoint metadata
    let path = match id {
        Identifier::PersistentId(pid) => {
            format!("api/datasets/:persistentId/add?persistentId={}", pid)
        }
        Identifier::Id(id) => format!("api/datasets/{}/add", id),
    };

    // Build context
    let body = assemble_upload_body(Some(body));
    let context = RequestType::Multipart {
        bodies: body,
        files: None,
        callbacks: None,
    };

    // Send the request
    // Add custom headers for the request
    let response = client
        .post(path.as_str(), None, context, Some(tagged_headers()))
        .await;

    evaluate_response::<DirectUploadResponse>(response).await
}

async fn register_batch_files(
    client: &BaseClient,
    id: &Identifier,
    body: impl Into<BatchDirectUploadBody>,
) -> Result<Response<BatchDirectUploadResponse>, String> {
    // Check if the dataset has any locks and wait until they are released
    check_lock!(client, &id);

    let path = match id {
        Identifier::PersistentId(pid) => {
            format!("api/datasets/:persistentId/addFiles?persistentId={}", pid)
        }
        Identifier::Id(id) => format!("api/datasets/{}/addFiles", id),
    };

    let body = assemble_upload_body(Some(body.into()));

    let context = RequestType::Multipart {
        bodies: body,
        files: None,
        callbacks: None,
    };

    let response = client
        .post(path.as_str(), None, context, Some(tagged_headers()))
        .await;

    evaluate_response::<BatchDirectUploadResponse>(response).await
}

/// Creates a header map with the `x-amz-tagging` header set to `dv-state=temp`.
///
/// This function returns a new `HeaderMap` with the `x-amz-tagging` header
/// set to the value `dv-state=temp`. This header is used to indicate that the
/// upload is in a temporary state and should be aborted if it fails.
///
/// # Returns
///
/// A `HeaderMap` with the `x-amz-tagging` header set to `dv-state=temp`.
fn tagged_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-tagging", HeaderValue::from_static("dv-state=temp"));

    headers
}

/// Cleans up expired uploads from the database.
///
/// Uploads that are at least expired 1 day are deleted. This is to prevent
/// the database from growing too much. This is called when direct upload
/// is started and when the upload is complete.
///
/// # Arguments
///
/// * `pool` - The database pool
///
/// # Returns
///
/// A `Result` containing either:
/// * `()` - Indicates successful cleanup
async fn cleanup_expired_and_complete_uploads(pool: &Pool<Sqlite>) -> Result<(), String> {
    let expiration_time = Utc::now().naive_utc() - Duration::days(1);
    Upload::delete_expired(&pool, Some(expiration_time))
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

/// This enum is used to determine the test mode for the upload.
///
/// We need to handle this because, when using a dockerized version of
/// Dataverse, the passed URL(s) will be using the `localstack` service
/// name, which will fail outside of the docker network.
///
/// # Variants
/// * `LocalStack` - Use LocalStack for the upload
/// * `Off` - Do not modify the URL
pub(crate) enum TestMode {
    /// Use LocalStack for the upload, replacing "localstack" with "localhost" in URLs
    LocalStack,
    /// Do not modify the URL (default mode)
    Off,
}

impl TestMode {
    /// Processes a URL based on the current test mode
    ///
    /// If in LocalStack mode, replaces "localstack" with "localhost" in the URL
    /// Otherwise, returns the URL unchanged
    ///
    /// # Arguments
    ///
    /// * `url` - The URL to process
    ///
    /// # Returns
    ///
    /// The processed URL as a String
    pub(crate) fn process_url(&self, url: &str) -> String {
        match self {
            TestMode::LocalStack => {
                let replaced = url.replace("localstack", "localhost");
                replaced
            }
            TestMode::Off => url.to_string(),
        }
    }
}

impl FromStr for TestMode {
    type Err = String;

    /// Creates a TestMode from a string
    ///
    /// # Arguments
    ///
    /// * `s` - The string to parse
    ///
    /// # Returns
    ///
    /// A Result containing either:
    /// * `TestMode` - The parsed test mode
    /// * `String` - An error message if parsing failed
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "LocalStack" => TestMode::LocalStack,
            _ => TestMode::Off,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        direct_upload::model::Part,
        prelude::dataset::get_dataset_meta,
        test_utils::{
            create_test_collection, create_test_dataset, extract_test_env, set_storage_backend,
        },
    };

    use super::*;

    /// Helper function to set up a direct upload test environment.
    /// Creates a test collection and dataset with the specified storage backend.
    ///
    /// # Arguments
    /// * `backend` - The storage backend to use (e.g., "LocalStack")
    ///
    /// # Returns
    /// * A tuple containing the configured BaseClient and the dataset's persistent ID
    async fn setup_direct_upload(backend: &str) -> (BaseClient, String) {
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).expect("Failed to create client");
        let collection_alias = create_test_collection(&client, "Root").await;
        let (_, pid) = create_test_dataset(&client, &collection_alias).await;
        set_storage_backend(&client, backend, &collection_alias).await;

        (client, pid)
    }

    /// Sets the test mode environment variable for the upload process
    fn set_test_mode(backend: &str) {
        std::env::set_var("TEST_MODE", backend);
    }

    #[tokio::test]
    async fn test_upload_single_part() {
        // ARRANGE
        // Set up test environment and client
        let (client, pid) = setup_direct_upload("LocalStack").await;
        set_test_mode("LocalStack");

        // Prepare test file and upload parameters
        let path = PathBuf::from("tests/fixtures/file.txt");
        let file = UploadFile::from_path(path)
            .await
            .expect("Failed to create upload file");

        let body = DirectUploadBody {
            file_name: Some(file.name.clone()),
            mime_type: Some("text/plain".to_string()),
            ..Default::default()
        };

        // ACT
        // Perform the file upload
        direct_upload(
            &client,
            Identifier::PersistentId(pid.clone()),
            file,
            "MD5",
            body,
            None,
        )
        .await
        .expect("Failed to upload file");

        // ASSERT
        // Verify the file was uploaded successfully
        let files = get_dataset_meta(&client, &Identifier::PersistentId(pid), &None)
            .await
            .expect("Failed to fetch dataset metadata");

        assert_eq!(
            files.data.expect("No data").files.len(),
            1,
            "Expected exactly one file in the dataset"
        );
    }

    #[tokio::test]
    async fn test_upload_multi_part() {
        // ARRANGE
        // Set up test environment and client
        let (client, pid) = setup_direct_upload("LocalStack").await;
        set_test_mode("LocalStack");

        // Prepare test file and calculate expected hash
        let file_path = PathBuf::from("tests/fixtures/multipart_upload.bin");
        let expected_hash = format!(
            "{:x}",
            md5::compute(std::fs::read(file_path.clone()).unwrap())
        );

        let body = DirectUploadBody {
            file_name: Some("large_test_file.bin".to_string()),
            mime_type: Some("text/plain".to_string()),
            ..Default::default()
        };

        // ACT
        // Perform the multi-part upload
        direct_upload(
            &client,
            Identifier::PersistentId(pid.clone()),
            file_path,
            "MD5",
            body,
            None,
        )
        .await
        .expect("Failed to upload file");

        // ASSERT
        // Verify file upload and checksum
        let files = get_dataset_meta(&client, &Identifier::PersistentId(pid), &None)
            .await
            .expect("Failed to fetch dataset metadata");

        let data = files.data.expect("No dataset data returned");
        assert_eq!(
            data.files.len(),
            1,
            "Expected exactly one file in the dataset"
        );

        let file = &data.files[0];
        let data_file = file.data_file.as_ref().expect("No data file present");
        let checksum = data_file.checksum.as_ref().expect("No checksum present");

        assert_eq!(
            checksum.value.as_ref().expect("No checksum value"),
            &expected_hash,
            "File checksum does not match expected value"
        );
    }

    #[tokio::test]
    async fn test_upload_large_file() {
        // ARRANGE
        // Set up test environment and client
        let (client, pid) = setup_direct_upload("LocalStack").await;
        set_test_mode("LocalStack");

        // Get the size from env variable
        // Default to 30MB if not specified, otherwise use the specified size
        let size = std::env::var("DVCLI_TEST_MULTIPART_SIZE")
            .unwrap_or("30000000".to_string()) // 30MB if not specified
            .parse::<usize>()
            .expect("Failed to parse multipart test size");

        // Mock a file with the specified size
        let data = vec![0; size];

        // Create directory if it doesn't exist
        std::fs::create_dir_all("tests/fixtures")
            .expect("Failed to create tests/fixtures directory");

        // Write test data to the file
        let file_path = PathBuf::from("tests/fixtures/direct_upload.txt");
        std::fs::write(&file_path, &data).expect("Failed to write data to test file");

        let expected_hash = format!("{:x}", md5::compute(std::fs::read(&file_path).unwrap()));

        let body = DirectUploadBody {
            file_name: Some("direct_upload.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            ..Default::default()
        };

        // ACT
        // Perform the multi-part upload
        direct_upload(
            &client,
            Identifier::PersistentId(pid.clone()),
            file_path,
            "MD5",
            body,
            None,
        )
        .await
        .expect("Failed to upload file");

        // ASSERT
        // Verify file upload and checksum
        let files = get_dataset_meta(&client, &Identifier::PersistentId(pid), &None)
            .await
            .expect("Failed to fetch dataset metadata");

        let data = files.data.expect("No dataset data returned");
        assert_eq!(
            data.files.len(),
            1,
            "Expected exactly one file in the dataset"
        );

        let file = &data.files[0];
        let data_file = file.data_file.as_ref().expect("No data file present");
        let checksum = data_file.checksum.as_ref().expect("No checksum present");

        assert_eq!(
            checksum.value.as_ref().expect("No checksum value"),
            &expected_hash,
            "File checksum does not match expected value"
        );
    }

    #[tokio::test]
    async fn test_batch_upload() {
        // ARRANGE
        // Set up test environment and client
        let (client, pid) = setup_direct_upload("LocalStack").await;
        set_test_mode("LocalStack");

        // Get the size from env variable
        // Default to 30MB if not specified, otherwise use the specified size
        let size = std::env::var("DVCLI_TEST_MULTIPART_SIZE")
            .unwrap_or("30000000".to_string()) // 30MB if not specified
            .parse::<usize>()
            .expect("Failed to parse multipart test size");

        // Mock a file with the specified size
        let data = vec![0; size];

        // Create directory if it doesn't exist
        std::fs::create_dir_all("tests/fixtures")
            .expect("Failed to create tests/fixtures directory");

        // Write test data to the file
        let file1_path = PathBuf::from("tests/fixtures/direct_upload.txt");
        std::fs::write(&file1_path, &data).expect("Failed to write data to test file");

        let file2_path = PathBuf::from("tests/fixtures/direct_upload2.txt");
        std::fs::write(&file2_path, &data).expect("Failed to write data to test file");

        let expected_hash1 = format!("{:x}", md5::compute(std::fs::read(&file1_path).unwrap()));
        let expected_hash2 = format!("{:x}", md5::compute(std::fs::read(&file2_path).unwrap()));
        let body1 = DirectUploadBody {
            file_name: Some("direct_upload.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            ..Default::default()
        };
        let body2 = DirectUploadBody {
            file_name: Some("direct_upload2.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            ..Default::default()
        };

        // ACT
        // Perform the multi-part upload
        let response = batch_direct_upload(
            &client,
            Identifier::PersistentId(pid.clone()),
            vec![file1_path, file2_path],
            "MD5",
            vec![body1, body2],
            2,
        )
        .await
        .expect("Failed to upload file");

        assert!(response.status.is_ok(), "Batch upload failed");

        // ASSERT
        // Verify file upload and checksum
        let files = get_dataset_meta(&client, &Identifier::PersistentId(pid), &None)
            .await
            .expect("Failed to fetch dataset metadata");

        let data = files.data.expect("No dataset data returned");
        assert_eq!(
            data.files.len(),
            2,
            "Expected exactly two files in the dataset"
        );

        let expected_hashes = vec![expected_hash1, expected_hash2];

        for (file, expected_hash) in data.files.iter().zip(expected_hashes.iter()) {
            let data_file = file.data_file.as_ref().expect("No data file present");
            let checksum = data_file.checksum.as_ref().expect("No checksum present");

            assert_eq!(
                checksum.value.as_ref().expect("No checksum value"),
                expected_hash,
                "File checksum does not match expected value"
            );
        }
    }

    #[tokio::test]
    #[should_panic]
    async fn test_duplicate_batch_upload() {
        // ARRANGE
        // Set up test environment and client
        let (client, pid) = setup_direct_upload("LocalStack").await;
        set_test_mode("LocalStack");

        // Get the size from env variable
        // Default to 30MB if not specified, otherwise use the specified size
        let size = std::env::var("DVCLI_TEST_MULTIPART_SIZE")
            .unwrap_or("30000000".to_string()) // 30MB if not specified
            .parse::<usize>()
            .expect("Failed to parse multipart test size");

        // Mock a file with the specified size
        let data = vec![0; size];

        // Create directory if it doesn't exist
        std::fs::create_dir_all("tests/fixtures")
            .expect("Failed to create tests/fixtures directory");

        // Write test data to the file
        let file1_path = PathBuf::from("tests/fixtures/direct_upload.txt");
        std::fs::write(&file1_path, &data).expect("Failed to write data to test file");

        let file2_path = PathBuf::from("tests/fixtures/direct_upload.txt");
        std::fs::write(&file2_path, &data).expect("Failed to write data to test file");

        let body1 = DirectUploadBody {
            file_name: Some("direct_upload.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            ..Default::default()
        };
        let body2 = DirectUploadBody {
            file_name: Some("direct_upload.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            ..Default::default()
        };

        // ACT
        // Perform the multi-part upload
        batch_direct_upload(
            &client,
            Identifier::PersistentId(pid.clone()),
            vec![file1_path, file2_path],
            "MD5",
            vec![body1, body2],
            2,
        )
        .await
        .expect("Failed to upload file");
    }

    #[tokio::test]
    async fn test_hash_file() {
        // ARRANGE
        let test_file_path = PathBuf::from("tests/fixtures/file.txt");
        let upload_file = UploadFile::from_path(test_file_path.clone())
            .await
            .expect("Failed to create upload file");

        let pool = setup_db().await;

        // Create a test upload record
        let upload = Upload::new(
            "https://example.com",
            "storage_id",
            "1234567890",
            test_file_path.to_str().unwrap(),
            "https://example.com/complete",
            "https://example.com/abort",
            100,
            10,
            "MD5",
        );

        // Insert the upload record without any parts
        Upload::insert(&pool, &upload, Vec::<Part>::new())
            .await
            .expect("Failed to insert upload record");

        // ACT
        let hasher = FileHash::new("MD5").expect("Failed to create MD5 hasher");
        hash_and_store(&upload, &pool, &hasher, upload_file)
            .await
            .expect("Failed to hash and store file");

        // ASSERT
        // Calculate expected hash directly from file contents
        let file_contents = std::fs::read(&test_file_path).expect("Failed to read test file");
        let expected_hash = format!("{:x}", md5::compute(file_contents));

        // Retrieve the updated upload record and verify the hash
        let updated_upload = Upload::get_by_hash(&pool, &upload.hash)
            .await
            .expect("Failed to retrieve upload record");

        assert_eq!(
            updated_upload.file_hash.expect("No file hash found"),
            expected_hash,
            "Stored file hash doesn't match expected value"
        );
    }

    /// Helper function to set up a test database.
    ///
    /// # Returns
    /// * A `sqlx::Pool<sqlx::Sqlite>` - The test database pool
    async fn setup_db() -> sqlx::Pool<sqlx::Sqlite> {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        Upload::create_table(&pool)
            .await
            .expect("Failed to create table");
        Part::create_table(&pool)
            .await
            .expect("Failed to create table");
        pool
    }
}
