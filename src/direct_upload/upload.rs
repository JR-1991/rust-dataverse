use std::collections::HashMap;
use std::str::FromStr;

use itertools::Itertools;
use reqwest::header::{HeaderMap, HeaderValue};
use typify::import_types;

use crate::check_lock;
use crate::client::{evaluate_response, BaseClient};
use crate::direct_upload::ticket::get_ticket;
use crate::file::uploadfile::UploadFile;
use crate::prelude::dataset::upload::assemble_upload_body;
use crate::prelude::{CallbackFun, Identifier};
use crate::request::RequestType;
use crate::response::Response;

use super::hasher::{FileHash, Hasher};
use super::ticket::{MultiPartTicket, SinglePartTicket, TicketResponse};

import_types!(
    schema = "models/direct_upload/upload.json",
    struct_builder = true,
    derives = [Default]
);

// pub async fn batch_direct_upload(
//     client: &BaseClient,
//     id: Identifier,
//     files: Vec<impl Into<UploadFile>>,
//     hasher: impl TryInto<FileHash, Error = String>,
//     body: Option<Vec<DirectUploadBody>>,
//     n_parallel: usize,
// ) {
// }

/// Uploads a file directly to a Dataverse dataset using the direct upload API.
///
/// This function handles the complete direct upload process which includes:
/// 1. Obtaining an upload ticket from the server
/// 2. Uploading the file content to the storage location specified in the ticket
/// 3. Computing file hashes during upload for integrity verification
/// 4. Registering the uploaded file with the dataset
///
/// The function supports both single-part and multi-part uploads based on the
/// ticket type returned by the server. The appropriate upload method is selected
/// automatically.
///
/// # Arguments
///
/// * `client` - The base client used for API communication
/// * `id` - Dataset identifier (either persistent ID or database ID)
/// * `file` - The file to be uploaded
/// * `hasher` - The hash algorithm to use for file integrity verification
/// * `body` - Upload metadata including file name, description, etc.
/// * `callbacks` - Optional vector of callback functions for progress tracking
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
    let hasher = hasher.try_into()?;
    let mut body = body;
    let file = file.into();
    // Fetch tickets
    let ticket_response = get_ticket(client, file.size as usize, id.clone())
        .await?
        .data
        .ok_or("No ticket data")?;

    match ticket_response {
        TicketResponse::SinglePartTicket(ticket) => {
            upload_single_part(client, &ticket, file, &hasher, callbacks).await?;
            body.storage_identifier = Some(ticket.storage_identifier);
        }
        TicketResponse::MultiPartTicket(ticket) => {
            upload_multi_part(client, &ticket, file, &hasher, callbacks).await?;
            body.storage_identifier = Some(ticket.storage_identifier.clone());
        }
    }

    // Add the hash to the body
    hasher.add_to_payload(&mut body).unwrap();

    register_file(client, id, body).await
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
) -> Result<(), String> {
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
        Ok(())
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
    ticket: &MultiPartTicket,
    file: UploadFile,
    hasher: &FileHash,
    callbacks: Option<Vec<CallbackFun>>,
) -> Result<(), String> {
    // Merge callbacks and hasher
    let callbacks = if let Some(mut cbs) = callbacks {
        cbs.push(hasher.to_callback());
        Some(cbs)
    } else {
        Some(vec![hasher.to_callback()])
    };

    // Sort the URLs by part number
    let urls = sort_urls_by_part_number(&ticket.urls);

    // Chunk the file and prepare for upload
    let chunked_files = file
        .chunk_file(ticket.part_size as u64)
        .await
        .map_err(|e| e.to_string())?;

    let mut e_tags = HashMap::new();

    // Upload each chunk
    for (file, (part_number, url)) in chunked_files.into_iter().zip(urls) {
        let tag = match upload_chunk(client, &url, file, callbacks.clone()).await {
            Ok(tag) => tag,
            Err(e) => {
                abort_upload(client, &ticket.abort).await?;
                return Err(e);
            }
        };

        e_tags.insert(part_number, tag);
    }

    // Complete the upload
    complete_upload(client, &ticket.complete, e_tags).await?;

    Ok(())
}

/// Sorts the URLs by part number.
///
/// This function is used to ensure that the parts are uploaded in the correct order.
///
/// # Arguments
///
/// * `urls` - A `HashMap` of part numbers and URLs
///
/// # Returns
///
/// A vector of tuples containing the part number and the corresponding URL
fn sort_urls_by_part_number(urls: &HashMap<String, String>) -> Vec<(String, String)> {
    urls.clone()
        .into_iter()
        .map(|(part_number, url)| (part_number, url))
        .collect::<Vec<_>>()
        .into_iter()
        .sorted_by(|(a, _), (b, _)| a.cmp(b))
        .collect()
}

/// Uploads a chunk of a file to the server.
///
/// This function uploads a chunk of a file to the server and returns the ETag
/// of the uploaded chunk.
///
/// # Arguments
///
/// * `client` - The base client used for API communication
/// * `url` - The URL to upload the chunk to
/// * `file` - The file to upload
/// * `callbacks` - Optional vector of callback functions for progress tracking
///
/// # Returns
///
/// A `Result` containing either:
/// * `String` - The ETag of the uploaded chunk
/// * `String` - An error message if the upload failed
async fn upload_chunk(
    client: &BaseClient,
    url: &str,
    file: UploadFile,
    callbacks: Option<Vec<CallbackFun>>,
) -> Result<String, String> {
    // Ensure we don't close the file handle prematurely
    let context = RequestType::File { file, callbacks };
    let url = TestMode::from_str(&std::env::var("TEST_MODE").unwrap_or_default())
        .unwrap_or(TestMode::Off)
        .process_url(url);

    let response = client.put(&url, None, context, None).await;

    match response {
        Err(e) => Err(e.to_string()),
        Ok(response) => {
            let headers = response.headers().clone();
            headers
                .get("ETag")
                .ok_or("No ETag header")?
                .to_str()
                .map_err(|e| e.to_string())
                .map(String::from)
        }
    }
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

/// Aborts the upload of a file by sending a DELETE request to the server.
///
/// This function sends a DELETE request to the server to abort the upload
/// process. It is used to clean up any resources associated with the upload
/// if it fails or is interrupted.
///
/// # Arguments
///
/// * `client` - The base client used for API communication
/// * `abort_url` - The URL to abort the upload
///
/// # Returns
///
/// A `Result` containing either:
/// * `()` - Indicates successful abort
/// * `String` - An error message if the abort failed
async fn abort_upload(client: &BaseClient, abort_url: &str) -> Result<(), String> {
    client
        .delete(&abort_url, None, RequestType::Plain, None)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
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
    fn process_url(&self, url: &str) -> String {
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

    use tempfile::NamedTempFile;

    use crate::{
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
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        std::fs::write(temp_file.path(), &data).expect("Failed to write data to temp file");

        let expected_hash = format!(
            "{:x}",
            md5::compute(std::fs::read(temp_file.path()).unwrap())
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
            temp_file.path().to_path_buf(),
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
}
