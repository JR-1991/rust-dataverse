//! Authentication-related CLI commands and profile management
//!
//! This module provides functionality for:
//! - Managing authentication profiles with server URLs and API tokens
//! - Securely storing credentials in the system keyring
//! - Validating authentication inputs like URLs and tokens
//! - Retrieving stored credentials for API requests

use keyring::{Entry, Result};
use structopt::StructOpt;
use url::Url;
use uuid::Uuid;

use crate::client::BaseClient;

use super::base::Matcher;

/// Subcommands for handling authentication in the Dataverse CLI
#[derive(StructOpt, Debug)]
#[structopt(about = "Handle authentication of the Dataverse CLI")]
pub enum AuthSubCommand {
    /// Set an authentication profile with a name, URL and token
    #[structopt(about = "Set the authentication profile")]
    Set {
        /// Name to identify this authentication profile
        #[structopt(short, long, help = "Name of the profile")]
        name: String,

        /// URL of the Dataverse server to authenticate against
        #[structopt(short, long, help = "URL of the Dataverse server")]
        url: String,

        /// API token used for authentication with the Dataverse server
        #[structopt(short, long, help = "API token for authentication")]
        token: String,
    },
}

/// Implementation of the Matcher trait for AuthSubCommand to process authentication commands
impl Matcher for AuthSubCommand {
    /// Process the authentication subcommand by storing credentials in the system keyring
    ///
    /// # Arguments
    /// * `_client` - The BaseClient instance (unused in this implementation)
    ///
    /// # Implementation Details
    /// This function:
    /// 1. Creates a new AuthProfile from the provided credentials
    /// 2. Attempts to store the profile in the system keyring
    /// 3. Prints success/failure message to the user
    fn process(self, _client: &BaseClient) {
        match self {
            AuthSubCommand::Set { name, url, token } => {
                let auth_profile =
                    AuthProfile::new(name.clone(), url.clone(), token.clone()).unwrap();
                let response = auth_profile.set_to_keyring();

                match response {
                    Ok(_) => println!("Profile set successfully"),
                    Err(e) => println!("Failed to set profile: {e}"),
                }
            }
        }
    }
}

/// A struct representing an authentication profile for the Dataverse CLI.
/// Contains a name for the profile, the Dataverse server URL, and an API token.
///
/// The AuthProfile provides methods for:
/// - Creating new profiles with validation
/// - Storing credentials securely in the system keyring
/// - Retrieving stored credentials
/// - Accessing profile components
#[derive(Debug)]
pub struct AuthProfile {
    /// Name identifier for the profile
    name: String,
    /// URL of the Dataverse server
    url: String,
    /// API token for authentication
    token: String,
}

impl AuthProfile {
    /// Creates a new AuthProfile instance with validation of inputs.
    ///
    /// # Arguments
    /// * `name` - The name of the profile
    /// * `url` - The Dataverse server URL
    /// * `token` - The API token for authentication
    ///
    /// # Returns
    /// A Result containing either:
    /// - Ok(AuthProfile): A new validated AuthProfile instance
    /// - Err(String): An error message if validation fails
    ///
    /// # Errors
    /// Returns an error if:
    /// - The URL is not a valid URL format
    /// - The token is not a valid UUID string
    pub fn new(name: String, url: String, token: String) -> std::result::Result<Self, String> {
        // Validate URL
        Url::parse(&url).map_err(|_| "Invalid URL format".to_string())?;

        // Validate UUID token
        Uuid::parse_str(&token)
            .map_err(|_| "Invalid token format - must be a valid UUID".to_string())?;

        Ok(AuthProfile { name, url, token })
    }

    /// Stores the profile credentials securely in the system keyring.
    ///
    /// # Implementation Details
    /// The credentials are stored as a combined string in the format "url--token"
    /// under the profile name as the key.
    ///
    /// # Returns
    /// A Result indicating:
    /// - Ok(()): Success storing the credentials
    /// - Err(keyring::Error): Failure accessing or writing to keyring
    ///
    /// # Errors
    /// Returns an error if:
    /// - Unable to access the system keyring
    /// - Unable to write to the keyring entry
    pub fn set_to_keyring(&self) -> Result<()> {
        let entry = Entry::new("dvcli", self.name.as_str())?;
        let combined = Self::combine_url_and_token(&self.url, &self.token);
        entry.set_password(combined.as_str())?;
        Ok(())
    }

    /// Retrieves profile credentials from the system keyring.
    ///
    /// # Arguments
    /// * `name` - The name of the profile to retrieve
    ///
    /// # Returns
    /// A Result containing either:
    /// - Ok(AuthProfile): The retrieved profile
    /// - Err(keyring::Error): Error accessing keyring
    ///
    /// # Errors
    /// Returns an error if:
    /// - Unable to access the system keyring
    /// - Unable to read the keyring entry
    /// - The stored data format is invalid
    pub fn get_from_keyring(name: &str) -> Result<Self> {
        let entry = Entry::new("dvcli", name)?;
        let combined = entry.get_password()?;
        let (url, token) = Self::split_url_and_token(&combined);
        Ok(Self {
            name: name.to_string(),
            url,
            token,
        })
    }

    /// Combines the URL and token into a single string for storage.
    ///
    /// # Arguments
    /// * `url` - The Dataverse server URL
    /// * `token` - The API token
    ///
    /// # Returns
    /// A combined string in the format "url--token"
    ///
    /// # Implementation Details
    /// Uses "--" as a delimiter between URL and token since URLs cannot contain "--"
    fn combine_url_and_token(url: &str, token: &str) -> String {
        format!("{url}--{token}")
    }

    /// Splits a combined URL and token string back into separate components.
    ///
    /// # Arguments
    /// * `token` - The combined string in the format "url--token"
    ///
    /// # Returns
    /// A tuple containing (url, token) as separate strings
    ///
    /// # Implementation Details
    /// Splits on the "--" delimiter used by combine_url_and_token()
    fn split_url_and_token(token: &str) -> (String, String) {
        let parts = token.split("--").collect::<Vec<&str>>();
        (parts[0].to_string(), parts[1].to_string())
    }

    /// Returns the name of the authentication profile.
    ///
    /// # Returns
    /// A string slice containing the profile name
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Returns the URL of the Dataverse server.
    ///
    /// # Returns
    /// A string slice containing the server URL
    pub fn get_url(&self) -> &str {
        &self.url
    }

    /// Returns the API token for authentication.
    ///
    /// # Returns
    /// A string slice containing the API token
    pub fn get_token(&self) -> &str {
        &self.token
    }
}
