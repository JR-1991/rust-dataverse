use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;

use serde::Serialize;
use serde_json;
use typify::import_types;

use crate::check_lock;
use crate::direct_upload::hasher::{FileHash, Hasher};
use crate::file::callback::CallbackFun;
use crate::file::uploadfile::UploadFile;
use crate::native_api::dataset::get_dataset_meta;
use crate::prelude::DatasetVersion;
use crate::{
    client::{evaluate_response, BaseClient},
    identifier::Identifier,
    request::RequestType,
    response::Response,
};

import_types!(schema = "models/file/filemeta.json", struct_builder = true);

/// Uploads a file to a dataset identified by either a persistent identifier (PID) or a numeric ID.
///
/// This asynchronous function sends a POST request to the API endpoint designated for adding files to a dataset.
/// The function constructs the API endpoint URL dynamically, incorporating the dataset's identifier. It sets up
/// the request context for a multipart request, including the file path, optional body metadata, and optional callback.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`, representing the unique identifier of the dataset to which the file will be uploaded.
/// * `fpath` - A `PathBuf` instance representing the file path of the file to be uploaded.
/// * `body` - An optional `UploadBody` struct instance containing additional metadata for the upload.
/// * `callback` - An optional `CallbackFun` instance for handling callbacks during the upload process.
///
/// # Returns
///
/// A `Result` wrapping a `Response<UploadResponse>`, which contains the HTTP response status and the deserialized
/// response data indicating the outcome of the upload operation, if the request is successful,
/// or a `String` error message on failure.
pub async fn upload_file_to_dataset(
    client: &BaseClient,
    id: Identifier,
    file: impl Into<UploadFile>,
    body: Option<UploadBody>,
    callbacks: Option<Vec<CallbackFun>>,
) -> Result<Response<UploadResponse>, String> {
    // Check if the dataset has any locks and wait until they are released
    check_lock!(client, &id);

    // Endpoint metadata
    let path = match id {
        Identifier::PersistentId(_) => "api/datasets/:persistentId/add".to_string(),
        Identifier::Id(id) => format!("api/datasets/{}/add", id),
    };

    // Build hash maps for the request
    let file: HashMap<String, UploadFile> = HashMap::from([("file".to_string(), file.into())]);
    let callbacks = callbacks.map(|c| HashMap::from([("file".to_string(), c)]));
    let body = assemble_upload_body(body);

    // Build the request context
    let context = RequestType::Multipart {
        bodies: body,
        files: Some(file),
        callbacks,
    };

    let response = send_file_upload_request(client, id, path, context).await;

    evaluate_response::<UploadResponse>(response).await
}

/// Assembles the upload body for a file upload request.
///
/// This function takes an optional `UploadBody` and converts it into a format suitable for inclusion
/// in a multipart request. If a body is provided, it is serialized to JSON and wrapped in a HashMap
/// with the key "jsonData".
///
/// # Arguments
///
/// * `body` - An optional `UploadBody` struct instance containing metadata for the upload.
///
/// # Returns
///
/// An `Option<HashMap<String, String>>` containing the serialized body if one was provided,
/// or `None` if no body was provided.
pub(crate) fn assemble_upload_body<T: Serialize>(
    body: Option<T>,
) -> Option<HashMap<String, String>> {
    body.as_ref()
        .map(|b| HashMap::from([("jsonData".to_string(), serde_json::to_string(&b).unwrap())]))
}

/// Sends a file upload request to the API.
///
/// This asynchronous function handles the details of sending a POST request to upload a file,
/// taking into account the type of identifier used (persistent ID or numeric ID).
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`.
/// * `path` - A `String` representing the API endpoint path.
/// * `context` - A `RequestType` enum instance containing the request details.
///
/// # Returns
///
/// A `Result` wrapping a `reqwest::Response` if the request is successful,
/// or a `reqwest::Error` if the request fails.
pub(crate) async fn send_file_upload_request(
    client: &BaseClient,
    id: Identifier,
    path: String,
    context: RequestType,
) -> Result<reqwest::Response, reqwest::Error> {
    match id {
        Identifier::PersistentId(id) => {
            client
                .post(
                    path.as_str(),
                    Some(HashMap::from([("persistentId".to_string(), id.clone())])),
                    context,
                    None,
                )
                .await
        }
        Identifier::Id(_) => client.post(path.as_str(), None, context, None).await,
    }
}

/// Checks if a file exists in a dataset by comparing its name and MD5 checksum.
///
/// This asynchronous function performs the following steps:
/// 1. Extracts the file name from the provided file path.
/// 2. Calculates the MD5 checksum of the file.
/// 3. Fetches the dataset metadata.
/// 4. Iterates over the files in the latest version of the dataset to find a match based on the file name and MD5 checksum.
///
/// # Arguments
///
/// * `client` - A reference to the `BaseClient` instance used to send the request.
/// * `id` - An `Identifier` enum instance, which can be either a `PersistentId(String)` or an `Id(i64)`, representing the unique identifier of the dataset.
/// * `fpath` - A `PathBuf` instance representing the file path of the file to be checked.
/// * `version` - An optional `DatasetVersion` enum instance specifying which version of the dataset to check. If not provided, the function will first try with the provided version, then fall back to the latest version.
///
/// # Returns
///
/// A `Result` wrapping a tuple of two `bool` values and an optional `i64`:
///
/// - The first value indicates whether the file exists in the dataset.
/// - The second value indicates whether the file with the same MD5 checksum exists in the dataset.
/// - The third value is an optional i64 file id.
///
/// # Errors
///
/// This function will return an error if:
/// - The file name cannot be extracted from the file path.
/// - The dataset metadata cannot be fetched.
/// - The latest version of the dataset or the files within it are not found.
/// - Any of the required fields (label, MD5 checksum) are missing in the dataset files.
pub async fn file_exists_at_dataset(
    client: &BaseClient,
    id: &Identifier,
    fpath: &PathBuf,
    version: Option<DatasetVersion>,
) -> Result<(bool, bool, Option<i64>), Box<dyn Error>> {
    let mut file_id: Option<i64> = None;
    let mut exists: bool = false;
    let mut same_hash: bool = false;

    let file_name = fpath
        .file_name()
        .ok_or("File name is not valid")?
        .to_str()
        .ok_or("File name is not valid")?;

    let md5_checksum = FileHash::new("MD5").and_then(|hasher| {
        let buffer = std::fs::read(fpath).unwrap();
        hasher.consume(&buffer).map_err(|e| e.to_string())?;
        hasher.compute().map_err(|e| e.to_string())
    })?;

    // Fetch the dataset metadata
    let mut dataset_response = get_dataset_meta(client, id, &version).await?;

    if dataset_response.status.is_err() {
        // Try again with Latest version
        dataset_response = get_dataset_meta(client, id, &Some(DatasetVersion::Latest)).await?;
    }

    let dataset = dataset_response
        .data
        .ok_or(format!("Dataset '{}' not found", id))?;

    for file in dataset.files {
        let label = file.label.ok_or("No label found")?;
        let data_file = file.data_file.ok_or("No 'data_file' found")?;
        let file_md5 = data_file.md5.ok_or("No MD5 checksum found")?;

        if label == file_name {
            if file_md5 == md5_checksum {
                same_hash = true;
            }

            file_id = data_file.id;
            exists = true;
            break;
        }
    }

    Ok((exists, same_hash, file_id))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::identifier::Identifier;
    use crate::prelude::dataset::upload::{file_exists_at_dataset, upload_file_to_dataset};
    use crate::prelude::BaseClient;
    use crate::test_utils::{create_test_dataset, extract_test_env, prepare_upload_body};

    /// Tests the file upload functionality to a dataset using a persistent identifier (PID).
    ///
    /// This test case demonstrates the process of uploading a file to a dataset identified by its PID.
    /// It involves setting up a client with API token and base URL obtained from environment variables,
    /// creating a test dataset within a specified parent dataverse ("Root"), and uploading a file to this dataset.
    /// The test asserts that the file upload operation was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the file upload operation fails, indicating an issue with the file upload process.
    #[tokio::test]
    async fn test_upload_file_to_dataset_with_pid() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Create a test file
        let fpath = PathBuf::from("tests/fixtures/file.txt");

        // Upload the file to the dataset
        let response =
            upload_file_to_dataset(&client, Identifier::PersistentId(pid), fpath, None, None)
                .await
                .expect("Failed to upload file to dataset");

        // Assert that the upload was successful
        assert!(response.status.is_ok());
    }

    /// Tests the file upload functionality to a dataset using a dataset ID.
    ///
    /// This test case demonstrates the process of uploading a file to a dataset identified by its dataset ID.
    /// It involves setting up a client with API token and base URL obtained from environment variables,
    /// creating a test dataset within a specified parent dataverse ("Root"), and uploading a file to this dataset.
    /// The test asserts that the file upload operation was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the file upload operation fails, indicating an issue with the file upload process.
    #[tokio::test]
    async fn test_upload_file_to_dataset_with_id() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (id, _) = create_test_dataset(&client, "Root").await;

        // Create a test file
        let fpath = PathBuf::from("tests/fixtures/file.txt");

        // Upload the file to the dataset
        let response = upload_file_to_dataset(&client, Identifier::Id(id), fpath, None, None)
            .await
            .expect("Failed to upload file to dataset");

        // Assert that the upload was successful
        assert!(response.status.is_ok());
    }

    /// Tests the file upload functionality to a dataset with additional metadata.
    ///
    /// This test demonstrates uploading a file to a dataset identified by a persistent identifier (PID),
    /// along with additional metadata specified in the body of the request. It sets up a client using API token
    /// and base URL obtained from environment variables, creates a test dataset within a specified parent dataverse
    /// ("Root"), and uploads a file to this dataset with additional metadata. The test asserts that the file upload
    /// operation, including the metadata submission, was successful.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if the file upload operation fails, indicating an issue with either the file upload
    /// process or the metadata submission.
    #[tokio::test]
    async fn test_upload_with_body() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Create a test file
        let fpath = PathBuf::from("tests/fixtures/file.txt");

        // Create a test body
        let body = prepare_upload_body();

        // Upload the file to the dataset
        let response = upload_file_to_dataset(
            &client,
            Identifier::PersistentId(pid),
            fpath,
            Some(body),
            None,
        )
        .await
        .expect("Failed to upload file to dataset");

        // Assert that the upload was successful
        assert!(response.status.is_ok());
    }

    /// Tests the behavior of the upload functionality with a non-existent file.
    ///
    /// This test aims to verify the system's behavior when attempting to upload a file that does not exist.
    /// It sets up a client using API token and base URL obtained from environment variables, creates a test dataset
    /// within a specified parent dataverse ("Root"), and then attempts to upload a non-existent file to this dataset.
    /// The test is expected to panic, indicating that the system correctly identifies the file as non-existent
    /// and fails the operation.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic, as it is expected to do so when the file upload operation fails due to the file not existing.
    #[tokio::test]
    #[should_panic]
    async fn test_upload_non_existent_file() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Create a test file
        let fpath = PathBuf::from("tests/fixtures/non_existent_file.txt");

        // Upload the file to the dataset
        upload_file_to_dataset(&client, Identifier::PersistentId(pid), fpath, None, None)
            .await
            .expect("Failed to upload file to dataset");
    }

    /// Tests the `file_exists_at_dataset` function to verify if a file exists in a dataset.
    ///
    /// This test performs the following steps:
    /// 1. Sets up the client using API token and base URL obtained from environment variables.
    /// 2. Creates a test dataset within a specified parent dataverse ("Root").
    /// 3. Uploads a test file to the created dataset.
    /// 4. Checks if the uploaded file exists in the dataset by comparing its name and MD5 checksum.
    /// 5. Asserts that the file exists in the dataset.
    ///
    /// # Environment Variables
    /// - `API_TOKEN`: The API token used for authentication with the API.
    /// - `BASE_URL`: The base URL of the instance.
    ///
    /// # Panics
    /// This test will panic if any of the following operations fail:
    /// - Setting up the client.
    /// - Creating the test dataset.
    /// - Uploading the test file to the dataset.
    /// - Checking if the file exists in the dataset.
    #[tokio::test]
    async fn test_file_exists_at_dataset() {
        // Set up the client
        let (api_token, base_url, _) = extract_test_env();
        let client = BaseClient::new(&base_url, Some(&api_token)).unwrap();

        // Create a test dataset
        let (_, pid) = create_test_dataset(&client, "Root").await;

        // Create a test file
        let fpath = PathBuf::from("tests/fixtures/file.txt");

        // Upload the file to the dataset
        upload_file_to_dataset(
            &client,
            Identifier::PersistentId(pid.clone()),
            fpath.clone(),
            None,
            None,
        )
        .await
        .expect("Failed to upload file to dataset");

        // Check if the file exists in the dataset
        let (exists, same_hash, _file_id) =
            file_exists_at_dataset(&client, &Identifier::PersistentId(pid), &fpath, None)
                .await
                .expect("Failed to check if file exists in dataset");

        // Assert that the file exists in the dataset
        assert!(exists);
        assert!(same_hash);
    }
}
