use std::collections::HashMap;

use atty::Stream;
use colored::Colorize;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::Client;
use reqwest::Url;
use serde::Deserialize;

use crate::request::RequestType;
use crate::response::Response;

#[derive(Debug, Clone)]
pub struct BaseClient {
    base_url: Url,
    api_token: Option<String>,
    client: Client,
}

impl BaseClient {
    pub(crate) fn has_api_token(&self) -> bool {
        self.api_token.is_some()
    }
}

// This is the base client that will be used to make requests to the API.
// Its acts as a wrapper around the reqwest::blocking::Client and provides
// methods to make GET, POST, PUT, and DELETE requests.
impl BaseClient {
    pub fn new(base_url: &str, api_token: Option<&String>) -> Result<Self, reqwest::Error> {
        let base_url = Url::parse(base_url).unwrap();
        let default_headers = Self::default_headers(api_token);

        // Create a client with increased timeouts for large file uploads
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(300)) // 5-minute timeout
            .connect_timeout(std::time::Duration::from_secs(60)) // 1-minute connect timeout
            .pool_idle_timeout(std::time::Duration::from_secs(90)) // Keep connections alive longer
            .pool_max_idle_per_host(10)
            .default_headers(default_headers)
            .build()?;

        Ok(BaseClient {
            base_url,
            api_token: api_token.map(|s| s.to_owned().to_string()),
            client,
        })
    }

    fn default_headers(api_token: Option<&String>) -> HeaderMap {
        let mut headers = HeaderMap::new();

        if let Some(api_token) = api_token {
            headers.insert(
                "X-Dataverse-key",
                api_token.parse().expect("Failed to parse API token"),
            );
        }

        // Add the default headers
        headers.insert("Connection", HeaderValue::from_static("keep-alive"));
        headers.insert("Accept", HeaderValue::from_static("*/*"));
        headers.insert("User-Agent", HeaderValue::from_static("dv-rs/0.1.0"));
        headers.insert(
            "Accept-Encoding",
            HeaderValue::from_static("gzip, deflate, br, zstd"),
        );

        headers
    }

    /// Get the base URL of the client
    ///
    /// # Returns
    ///
    /// A reference to the base URL of the client
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub async fn get(
        &self,
        path: &str,
        parameters: Option<HashMap<String, String>>,
        context: RequestType,
        header_map: Option<HeaderMap>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.perform_request(reqwest::Method::GET, path, parameters, context, header_map)
            .await
    }

    pub async fn post(
        &self,
        path: &str,
        parameters: Option<HashMap<String, String>>,
        context: RequestType,
        header_map: Option<HeaderMap>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.perform_request(reqwest::Method::POST, path, parameters, context, header_map)
            .await
    }

    pub async fn put(
        &self,
        path: &str,
        parameters: Option<HashMap<String, String>>,
        context: RequestType,
        header_map: Option<HeaderMap>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.perform_request(reqwest::Method::PUT, path, parameters, context, header_map)
            .await
    }

    pub async fn delete(
        &self,
        path: &str,
        parameters: Option<HashMap<String, String>>,
        context: RequestType,
        header_map: Option<HeaderMap>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.perform_request(
            reqwest::Method::DELETE,
            path,
            parameters,
            context,
            header_map,
        )
        .await
    }

    pub async fn patch(
        &self,
        path: &str,
        parameters: Option<HashMap<String, String>>,
        context: RequestType,
        header_map: Option<HeaderMap>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.perform_request(
            reqwest::Method::PATCH,
            path,
            parameters,
            context,
            header_map,
        )
        .await
    }

    async fn perform_request(
        &self,
        method: reqwest::Method,
        path: &str,
        parameters: Option<HashMap<String, String>>,
        context: RequestType,
        header_map: Option<HeaderMap>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        // Process the URL and build the request based on the context
        let url = self.base_url.join(path).unwrap();

        // If the DEBUG environment variable is set, print the URL
        if std::env::var("DEBUG").is_ok() {
            print_call(url.to_string());
        }

        let mut headers = header_map.unwrap_or_default();

        if let RequestType::File { file, .. } = &context {
            let size = file.get_size().await.expect("Failed to get file size");

            if size > 0 {
                // This is the case in local files, but we cannot use it for
                // remote or receiver stream files because the size is unknown
                headers.insert("Content-Length", size.to_string().parse().unwrap());
            }
        }

        let request = context
            .to_request(self.client.request(method, url.clone()))
            .await
            .expect("Failed to build request");

        let request = match parameters {
            Some(parameters) => request.query(&parameters),
            None => request,
        };

        request.headers(headers).send().await
    }
}

// Helper function to evaluate a response
pub async fn evaluate_response<T>(
    response: Result<reqwest::Response, reqwest::Error>,
) -> Result<Response<T>, String>
where
    T: for<'de> Deserialize<'de>,
{
    // Check if the response is an error
    let response = match response {
        Ok(response) => response,
        Err(err) => {
            print_error(err.to_string());
            return Err(err.to_string());
        }
    };

    // Try to read the response into the response struct
    let raw_content = response.text().await.unwrap();
    let json = serde_json::from_str::<Response<T>>(&raw_content);

    match json {
        Ok(json) => Ok(json),
        Err(err) => {
            print_error(
                format!(
                    "{} - {}",
                    err.to_string().red().bold(),
                    raw_content.red().bold(),
                )
                .to_string(),
            );
            panic!("{}", err.to_string());
        }
    }
}

pub(crate) fn print_error(error: String) {
    println!("\n{} {}\n", "Error:".red().bold(), error,);
}

fn print_call(url: String) {
    if atty::is(Stream::Stdout) {
        println!("{}: {}", "Calling".to_string().blue().bold(), url);
    }
}

#[cfg(test)]
mod tests {
    use httpmock::prelude::*;
    use lazy_static::lazy_static;
    use serde::Serialize;

    use super::*;

    lazy_static! {
        static ref MOCK_SERVER: MockServer = MockServer::start();
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct ExampleBody {
        key1: String,
        key2: String,
    }

    impl PartialEq for ExampleBody {
        fn eq(&self, other: &Self) -> bool {
            self.key1 == other.key1 && self.key2 == other.key2
        }
    }

    impl Clone for ExampleBody {
        fn clone(&self) -> Self {
            ExampleBody {
                key1: self.key1.clone(),
                key2: self.key2.clone(),
            }
        }
    }

    #[tokio::test]
    async fn test_get_request() {
        let client = BaseClient::new(&MOCK_SERVER.base_url(), None).unwrap();

        let _m = MOCK_SERVER.mock(|when, then| {
            when.method(GET).path("/test");
            then.status(200).body("test");
        });

        let response = client.get("test", None, RequestType::Plain, None).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_json_body_request() {
        // Arrange
        let client = BaseClient::new(&MOCK_SERVER.base_url(), None).unwrap();
        let expected_body = ExampleBody {
            key1: "value1".to_string(),
            key2: "value2".to_string(),
        };

        let raw_body = serde_json::to_string(&expected_body).unwrap();
        let mock = MOCK_SERVER.mock(|when, then| {
            when.method(POST).path("/test_json");
            then.status(200).json_body(raw_body.clone());
        });

        // Act
        let response = client
            .post(
                "test_json",
                None,
                RequestType::JSON { body: raw_body },
                None,
            )
            .await;

        // Assert
        assert!(response.is_ok());

        mock.assert();
    }

    #[tokio::test]
    async fn test_multipart_request() {
        let client = BaseClient::new(&MOCK_SERVER.base_url(), None).unwrap();

        let mock = MOCK_SERVER.mock(|when, then| {
            when.method(POST).path("/test_multipart");
            then.status(200).body("test");
        });

        // Mock the body
        let expected_body = serde_json::json!({
            "key1": "value1",
            "key2": "value2"
        });

        let context = RequestType::Multipart {
            bodies: Some(HashMap::from([(
                "body".to_string(),
                expected_body.to_string(),
            )])),
            files: Some(HashMap::from([(
                "file".to_string(),
                "tests/fixtures/file.txt".into(),
            )])),
            callbacks: None,
        };

        // Act
        let response = client.post("test_multipart", None, context, None).await;

        // Assert
        assert!(response.is_ok());

        mock.assert();
    }

    #[tokio::test]
    async fn test_parameter_request() {
        let client = BaseClient::new(&MOCK_SERVER.base_url(), None).unwrap();

        let mock = MOCK_SERVER.mock(|when, then| {
            when.method(GET)
                .path("/test_parameters")
                .query_param("key1", "value1")
                .query_param("key2", "value2");
            then.status(200).body("test");
        });

        let parameters = Some(HashMap::from([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]));

        let response = client
            .get("test_parameters", parameters, RequestType::Plain, None)
            .await;

        assert!(response.is_ok());

        mock.assert();
    }
}
