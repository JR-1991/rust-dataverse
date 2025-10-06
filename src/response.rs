use atty::Stream;
use colored::Colorize;
use colored_json::prelude::*;

/// Represents the status of a response from the Dataverse API.
///
/// We distinguish success and error responses with this enum.
/// Once the response is parsed, we can check if it's an error or not
/// and act accordingly.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum Status {
    /// Indicates a successful response
    OK,
    /// Indicates an error response
    ERROR,
}

impl PartialEq for Status {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Status::OK, Status::OK) | (Status::ERROR, Status::ERROR)
        )
    }
}

impl Status {
    /// Returns the string representation of the status
    pub fn as_str(&self) -> &str {
        match self {
            Status::OK => "OK",
            Status::ERROR => "ERROR",
        }
    }

    /// Returns true if the status is OK
    pub fn is_ok(&self) -> bool {
        match self {
            Status::OK => true,
            Status::ERROR => false,
        }
    }

    /// Returns true if the status is ERROR
    pub fn is_err(&self) -> bool {
        match self {
            Status::OK => false,
            Status::ERROR => true,
        }
    }
}

/// A wrapper struct that models the response expected from Dataverse.
///
/// This struct contains the response status, optional data payload,
/// message, and request metadata.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[allow(non_snake_case)]
pub struct Response<T> {
    /// The status of the response (OK or ERROR)
    pub status: Status,

    /// Optional data payload returned by the API
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,

    /// Optional message providing additional information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<Message>,

    /// Optional URL of the request that generated this response
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requestUrl: Option<String>,

    /// Optional HTTP method used in the request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requestMethod: Option<String>,
}

impl<T> Response<T> {
    /// Creates a new Response from a message and status
    pub fn from_message(message: Message, status: Status) -> Self {
        Self {
            status,
            data: None,
            message: Some(message),
            requestUrl: None,
            requestMethod: None,
        }
    }

    /// Creates a new Response from a string into the data field
    pub fn from_string(data: String, status: Status) -> Response<String> {
        Response::<String> {
            status,
            data: Some(data),
            message: None,
            requestUrl: None,
            requestMethod: None,
        }
    }

    /// Creates a new Response
    pub fn new(status: Status, data: Option<T>, message: Option<Message>) -> Response<T> {
        Response::<T> {
            status,
            data,
            message,
            requestUrl: None,
            requestMethod: None,
        }
    }
}

impl<T> Response<T>
where
    T: serde::Serialize,
{
    /// Prints the response result to stdout and exits with appropriate code
    pub fn print_result(&self) {
        match self.status {
            Status::OK => {
                let json = serde_json::to_string_pretty(&self.data).unwrap();

                self.redirect_stream(&json);
                std::process::exit(exitcode::OK);
            }
            Status::ERROR => {
                println!(
                    "\n{} {}\n",
                    "Error:".red().bold(),
                    self.message.as_ref().unwrap()
                );
                std::process::exit(exitcode::DATAERR);
            }
        }
    }

    /// Redirects output to appropriate stream based on context
    ///
    /// If users are redirecting the output to a file, we don't want to print
    /// the success message but only the JSON response to ensure that the output
    /// is clean and can be used in other scripts
    fn redirect_stream(&self, json_str: &str) {
        if atty::is(Stream::Stdout) {
            println!("{}", success_message());
            println!("{}\n", json_str.to_colored_json_auto().unwrap());
        } else {
            println!("{}", json_str);
        }
    }
}

/// Returns a formatted success message string
fn success_message() -> String {
    format!(
        "{} {} - Received the following response: \n",
        "└── ".bold(),
        "🎉 Success!".green().bold()
    )
}

/// Represents a message that can be either plain text or nested.
///
/// This is a workaround to tackle the issue of having a nested message
/// in the response currently caused by the editMetadata endpoint.
///
/// For more info:
/// https://dataverse.zulipchat.com/#narrow/stream/378866-troubleshooting/topic/.E2.9C.94.20Duplicate.20file.20response
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Message {
    /// A simple string message
    PlainMessage(String),
    /// A message wrapped in a nested structure
    NestedMessage(NestedMessage),
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Message::PlainMessage(message) => write!(f, "{}", message),
            Message::NestedMessage(nested_message) => {
                write!(f, "{}", nested_message.message.as_ref().unwrap())
            }
        }
    }
}

/// Represents a nested message structure returned by some Dataverse endpoints
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct NestedMessage {
    /// The actual message content
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

impl std::fmt::Display for NestedMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message.as_ref().unwrap())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_status_eq() {
        let ok = super::Status::OK;
        let error = super::Status::ERROR;

        assert_eq!(ok, ok);
        assert_eq!(error, error);
        assert_ne!(ok, error);
    }

    #[test]
    fn test_status_as_str() {
        let ok = super::Status::OK;
        let error = super::Status::ERROR;

        assert_eq!(ok.as_str(), "OK");
        assert_eq!(error.as_str(), "ERROR");
    }

    #[test]
    fn test_status_is_ok() {
        let ok = super::Status::OK;
        let error = super::Status::ERROR;

        assert!(ok.is_ok());
        assert!(!error.is_ok());
    }

    #[test]
    fn test_status_is_err() {
        let ok = super::Status::OK;
        let error = super::Status::ERROR;

        assert!(!ok.is_err());
        assert!(error.is_err());
    }

    #[test]
    fn test_response_print_result() {
        let response = super::Response {
            status: super::Status::OK,
            data: Some("data"),
            message: None,
            requestUrl: None,
            requestMethod: None,
        };

        response.print_result();
    }

    #[test]
    fn test_message_display() {
        let plain_message = super::Message::PlainMessage("plain message".to_string());
        let nested_message = super::Message::NestedMessage(super::NestedMessage {
            message: Some("nested message".to_string()),
        });

        assert_eq!(format!("{}", plain_message), "plain message");
        assert_eq!(format!("{}", nested_message), "nested message");
    }

    #[test]
    fn test_nested_message_display() {
        let nested_message = super::NestedMessage {
            message: Some("nested message".to_string()),
        };

        assert_eq!(format!("{}", nested_message), "nested message");
    }
}
