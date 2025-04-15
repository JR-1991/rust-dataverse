use std::collections::HashMap;
use std::path::PathBuf;

use crate::file::callback::CallbackFun;
use crate::file::uploadfile::UploadFile;
use crate::file::zipstream::setup_recursive_zip_stream;
use crate::native_api::dataset::upload::{UploadBody, UploadResponse};
use crate::prelude::dataset::upload::{assemble_upload_body, send_file_upload_request};
use crate::{
    client::{evaluate_response, BaseClient},
    identifier::Identifier,
    request::RequestType,
    response::Response,
};

/// Uploads a directory to the server.
///
/// This function zips the contents of the specified directory and uploads it to the server.
/// It supports optional callback functions to track the upload progress.
///
/// # Arguments
/// * `client` - A reference to the `BaseClient` instance.
/// * `id` - The identifier of the dataset.
/// * `dir_path` - The path to the directory to be uploaded.
/// * `body` - An optional `UploadBody` containing additional data for the upload.
/// * `callback` - An optional callback function to track the upload progress.
///
/// # Returns
/// A `Result` containing a `Response` with the `UploadResponse` or an error message.
pub async fn upload_directory(
    client: &BaseClient,
    id: Identifier,
    dir_path: PathBuf,
    body: Option<UploadBody>,
    callback: Option<Vec<CallbackFun>>,
) -> Result<Response<UploadResponse>, String> {
    if !dir_path.is_dir() {
        return Err("The provided path is not a directory".to_string());
    }

    // Endpoint metadata
    let path = match id {
        Identifier::PersistentId(_) => "api/datasets/:persistentId/add".to_string(),
        Identifier::Id(id) => format!("api/datasets/{}/add", id),
    };

    // Prepare zipping channel
    let (writer_task, rx) = setup_recursive_zip_stream(dir_path);

    // Build hash maps for the request
    let file: HashMap<String, UploadFile> = HashMap::from([("file".to_string(), rx.into())]);
    let callbacks = callback.map(|c| HashMap::from([("file".to_string(), c)]));
    let body = assemble_upload_body(body);

    // Build the request context
    let context = RequestType::Multipart {
        bodies: body,
        files: Some(file),
        callbacks,
    };

    let response = send_file_upload_request(client, id, path, context).await;

    match writer_task.await {
        Ok(_) => (),
        Err(e) => return Err(e.to_string()),
    }

    evaluate_response::<UploadResponse>(response).await
}
