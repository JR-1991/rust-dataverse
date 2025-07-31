use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use indicatif::MultiProgress;
use reqwest::multipart::Part;
use reqwest::{multipart, Body, RequestBuilder};

use crate::file::callback::CallbackFun;
use crate::file::uploadfile::{FileSource, UploadFile};
use crate::progress::setup_progress_log;

pub enum RequestType {
    /// A plain request with no body.
    Plain,

    /// Raw request body with non-json and non-file content.
    Raw(String),

    /// A JSON request with a JSON body and the content type set to application/json.
    JSON { body: String },

    /// A multipart request with a body and files.
    Multipart {
        bodies: Option<HashMap<String, String>>,
        files: Option<HashMap<String, UploadFile>>,
        callbacks: Option<HashMap<String, Vec<CallbackFun>>>,
    },

    /// A File request with a file.
    File {
        file: UploadFile,
        callbacks: Option<Vec<CallbackFun>>,
    },
}

impl RequestType {
    /// Convert the request type to a request builder.
    ///
    /// # Arguments
    /// * `self` - The request type.
    /// * `request` - The request builder.
    ///
    /// # Returns
    /// A `Result` containing the modified request builder or an error.
    pub async fn to_request(
        self,
        request: RequestBuilder,
    ) -> Result<RequestBuilder, Box<dyn Error>> {
        match self {
            RequestType::Plain => Ok(request),
            RequestType::Raw(body) => Self::build_raw_request(&body, request),
            RequestType::JSON { body } => Self::build_json_request(&body, request),
            RequestType::Multipart {
                files,
                bodies,
                callbacks,
            } => Self::build_form_request(bodies, files, request, callbacks.clone()).await,
            RequestType::File { file, callbacks } => {
                Self::build_file_request(file, callbacks, request).await
            }
        }
    }

    /// Build a JSON request.
    ///
    /// # Arguments
    /// * `body` - The JSON body as a string.
    /// * `request` - The request builder.
    ///
    /// # Returns
    /// A `Result` containing the modified request builder or an error.
    fn build_json_request(
        body: &str,
        request: RequestBuilder,
    ) -> Result<RequestBuilder, Box<dyn Error>> {
        Ok(request
            .header("Content-Type", "application/json")
            .body(body.to_owned()))
    }

    /// Build a raw request with a string body.
    ///
    /// # Arguments
    /// * `body` - The raw body as a string.
    /// * `request` - The request builder.
    ///
    /// # Returns
    /// A `Result` containing the modified request builder or an error.
    fn build_raw_request(
        body: &str,
        request: RequestBuilder,
    ) -> Result<RequestBuilder, Box<dyn Error>> {
        Ok(request.body(body.to_owned()))
    }

    /// Build a multipart form request.
    ///
    /// # Arguments
    /// * `bodies` - Optional map of body parts.
    /// * `files` - Optional map of files to be uploaded.
    /// * `request` - The request builder.
    /// * `callbacks` - Optional map of callback functions.
    ///
    /// # Returns
    /// A `Result` containing the modified request builder or an error.
    async fn build_form_request(
        bodies: Option<HashMap<String, String>>,
        files: Option<HashMap<String, UploadFile>>,
        request: RequestBuilder,
        callbacks: Option<HashMap<String, Vec<CallbackFun>>>,
    ) -> Result<RequestBuilder, Box<dyn Error>> {
        let mut form = multipart::Form::new();

        if let Some(bodies) = bodies {
            for (key, value) in bodies {
                form = form.part(key.clone(), Part::text(value.clone()));
            }
        }

        if let Some(files) = files {
            for (key, file) in files {
                let part = Self::assemble_file_part(&callbacks, &key, file).await?;
                form = form.part(key, part);
            }
        }

        Ok(request.multipart(form))
    }

    async fn build_file_request(
        file: UploadFile,
        callbacks: Option<Vec<CallbackFun>>,
        request: RequestBuilder,
    ) -> Result<RequestBuilder, Box<dyn Error>> {
        let body = Self::prepare_file_stream(callbacks, file).await?;
        Ok(request.body(body))
    }

    /// Assemble a file part for a multipart form request.
    ///
    /// # Arguments
    /// * `callbacks` - Optional map of callback functions.
    /// * `key` - The key for the file part.
    /// * `file` - The file to be uploaded.
    ///
    /// # Returns
    /// A `Result` containing the file part or an error.
    async fn assemble_file_part(
        callbacks: &Option<HashMap<String, Vec<CallbackFun>>>,
        key: &str,
        file: UploadFile,
    ) -> Result<Part, Box<dyn Error>> {
        let callback = match &callbacks {
            Some(callbacks) => callbacks.get(key).cloned(),
            None => None,
        };

        let name = file.name.clone();
        let body = Self::prepare_file_stream(callback, file).await?;

        Part::stream(body)
            .file_name(name)
            .mime_str("application/octet-stream")
            .map_err(|e| e.into())
    }

    async fn prepare_file_stream(
        callbacks: Option<Vec<CallbackFun>>,
        file: UploadFile,
    ) -> Result<Body, Box<dyn Error>> {
        let multi_pb = Arc::new(MultiProgress::new());
        let size = file.get_size().await?;
        let pb = multi_pb.add(setup_progress_log(size, None, &file.name));
        file.create_body(callbacks, pb).await
    }

    /// Get the size of a file.
    ///
    /// # Arguments
    /// * `file` - The file whose size is to be determined.
    ///
    /// # Returns
    /// A `Result` containing the file size or an error.
    #[allow(dead_code)]
    async fn get_file_size(file: &UploadFile) -> Result<u64, Box<dyn Error>> {
        match &file.file {
            FileSource::RemoteUrl(url) => {
                let response = reqwest::Client::new().head(url.clone()).send().await?;
                let size = response
                    .headers()
                    .get("Content-Length")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);

                Ok(size)
            }
            _ => Ok(file.size),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use reqwest::Client;

    use crate::client::BaseClient;
    use crate::identifier::Identifier;
    use crate::native_api::dataset::upload_file_to_dataset;
    use crate::test_utils::{create_test_dataset, extract_test_env};

    use super::*;

    /// Test converting a plain request type to a request builder.
    #[tokio::test]
    async fn test_request_type_to_request_plain() {
        // Arrange
        let request = RequestType::Plain
            .to_request(Client::new().request(reqwest::Method::GET, "http://localhost"))
            .await
            .expect("Could not convert request");

        // Act
        let request = request.build().expect("Could not build request");

        assert_eq!(request.url().as_str(), "http://localhost/");
        assert_eq!(request.method(), reqwest::Method::GET);
    }

    /// Test converting a JSON request type to a request builder.
    #[tokio::test]
    async fn test_request_type_to_request_json() {
        // Arrange
        let request = RequestType::JSON {
            body: "{}".to_string(),
        }
        .to_request(Client::new().request(reqwest::Method::GET, "http://localhost"))
        .await
        .expect("Could not convert request");

        // Act
        let request = request.build().expect("Could not build request");

        // Assert
        assert_eq!(request.url().as_str(), "http://localhost/");
        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(
            request
                .body()
                .expect("Could not get body")
                .as_bytes()
                .expect("Could not get bytes"),
            "{}".as_bytes()
        );
        assert_eq!(
            request.headers().get("Content-Type").unwrap(),
            "application/json"
        );
    }

    /// Test converting a multipart form request type to a request builder.
    #[tokio::test]
    async fn test_request_type_to_request_form() {
        // Arrange
        let context = RequestType::Multipart {
            bodies: Some(HashMap::from([("body".to_string(), "{}".to_string())])),
            callbacks: None,
            files: Some(HashMap::from([(
                "file".to_string(),
                "tests/fixtures/file.txt".into(),
            )])),
        };

        let request = context
            .to_request(Client::new().request(reqwest::Method::GET, "http://localhost"))
            .await
            .expect("Could not convert request");

        // Act
        let request = request.build().expect("Could not build request");

        // Assert
        assert_eq!(request.url().as_str(), "http://localhost/");
        assert_eq!(request.method(), reqwest::Method::GET);
        assert!(request
            .headers()
            .get("Content-Type")
            .expect("Content-Type not found")
            .to_str()
            .unwrap()
            .contains("multipart/form-data"));
        assert!(
            request.body().is_some(),
            "Body not found in request: {request:?}"
        );
    }

    // Test converting a file request type to a request builder.
    #[tokio::test]
    async fn test_request_type_to_request_file() {
        // Arrange
        let context = RequestType::File {
            file: "tests/fixtures/file.txt".into(),
            callbacks: None,
        };

        let request = context
            .to_request(Client::new().request(reqwest::Method::GET, "http://localhost"))
            .await
            .expect("Could not convert request");

        // Act
        let request = request.build().expect("Could not build request");

        // Assert
        assert_eq!(request.url().as_str(), "http://localhost/");
        assert_eq!(request.method(), reqwest::Method::GET);
        assert!(
            request.body().is_some(),
            "Body not found in request: {request:?}"
        );
    }

    // Test getting the file size of a file.
    #[tokio::test]
    async fn test_get_remote_file_size() {
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

        // Now retrieve the Database ID of the uploaded file
        let data = response.data.clone();
        let file = data
            .unwrap()
            .files
            .first()
            .unwrap()
            .clone()
            .data_file
            .unwrap();

        let path: UploadFile = format!("{}/api/access/datafile/{}", base_url, file.id.unwrap())
            .as_str()
            .into();

        // Act
        let size = RequestType::get_file_size(&path)
            .await
            .expect("Could not get file size");

        // Assert
        assert!(size > 0, "File size is not greater than 0");
    }
}
