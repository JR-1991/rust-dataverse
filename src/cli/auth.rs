//! Authentication-related CLI commands and profile management
//!
//! This module provides functionality for:
//! - Managing authentication profiles with server URLs and API tokens
//! - Securely storing credentials in the system keyring
//! - Validating authentication inputs like URLs and tokens
//! - Retrieving stored credentials for API requests

use std::io::{self, Write};

use colored::Colorize;
use dialoguer::Input;
use keyring::{Entry, Result};
use rpassword::prompt_password;
use structopt::StructOpt;
use url::Url;
use uuid::Uuid;

use crate::client::BaseClient;

use super::base::Matcher;

/// Interactively prompts the user for Dataverse URL and API token
///
/// # Returns
/// A tuple containing (url, token) where:
/// - url: String - The Dataverse server URL
/// - token: String - The API token (may be empty if user skips)
///
/// # Errors
/// Returns an error if there are issues with user input or I/O operations
pub fn prompt_for_credentials() -> std::result::Result<(String, String), Box<dyn std::error::Error>>
{
    println!(
        "\n{}",
        "🔗 Setting up Dataverse connection...".bold().cyan()
    );
    println!("{}", "─".repeat(50).dimmed());

    // Get URL interactively
    let base_url: String = Input::new()
        .with_prompt(format!(
            "{} {}",
            "🌐".bold(),
            "Enter Dataverse server URL".bold().green()
        ))
        .default("https://demo.dataverse.org".to_string())
        .show_default(true)
        .interact_text()?;

    // Get token interactively with masking
    println!(
        "\n{} {}",
        "🔑".bold(),
        "Enter API token (optional - press Enter to skip)"
            .bold()
            .green()
    );
    println!("{}", "   Token will be hidden for security".dimmed());
    print!("{} ", "Token:".bold().yellow());
    io::stdout().flush()?;
    let api_token = prompt_password("")?;

    if !api_token.trim().is_empty() {
        println!("{}", "✓ Token received".green());
    } else {
        println!(
            "{}",
            "⚠ No token provided - some operations may be limited".yellow()
        );
    }

    println!("{}", "─".repeat(50).dimmed());
    Ok((base_url, api_token))
}

/// Subcommands for handling authentication in the Dataverse CLI
#[derive(StructOpt, Debug)]
#[structopt(about = "Handle authentication of the Dataverse CLI")]
pub enum AuthSubCommand {
    /// Set an authentication profile with a name, URL and token
    #[structopt(about = "Set the authentication profile")]
    Set {
        /// Name to identify this authentication profile
        #[structopt(short, long, help = "Name of the profile")]
        name: Option<String>,

        /// URL of the Dataverse server to authenticate against
        #[structopt(short, long, help = "URL of the Dataverse server")]
        url: Option<String>,

        /// API token used for authentication with the Dataverse server
        #[structopt(short, long, help = "API token for authentication")]
        token: Option<String>,
    },
}

/// Retrieves the profile name for authentication configuration.
///
/// This function efficiently handles profile name acquisition by either returning
/// the provided parameter directly or prompting the user through an interactive
/// interface. The interactive mode provides clear visual feedback and consistent
/// styling to enhance the user experience during profile setup.
///
/// # Arguments
///
/// * `name` - An optional string containing the profile name
///
/// # Returns
///
/// A `Result` containing the profile name or an error from user interaction
fn get_profile_name(
    name: Option<String>,
) -> std::result::Result<String, Box<dyn std::error::Error>> {
    if let Some(profile_name) = name {
        return Ok(profile_name);
    }

    println!("\n{}", "📝 Profile Setup".bold().cyan());
    println!("{}", "─".repeat(30).dimmed());

    Input::new()
        .with_prompt(format!(
            "{} {}",
            "👤".bold(),
            "Enter profile name".bold().green()
        ))
        .interact_text()
        .map_err(Into::into)
}

/// Gets credentials, using provided values or prompting interactively
fn get_credentials(
    url: Option<String>,
    token: Option<String>,
) -> std::result::Result<(String, String), Box<dyn std::error::Error>> {
    match (&url, &token) {
        (Some(u), Some(t)) => Ok((u.clone(), t.clone())),
        _ => {
            let (interactive_url, interactive_token) = prompt_for_credentials()?;
            let final_url = url.unwrap_or(interactive_url);
            let final_token = token.unwrap_or(interactive_token);
            Ok((final_url, final_token))
        }
    }
}

/// Creates and stores an authentication profile
fn create_and_store_profile(name: String, url: String, token: String) {
    println!("\n{}", "💾 Saving profile...".bold().cyan());

    match AuthProfile::new(name.clone(), url, token) {
        Ok(profile) => match profile.set_to_keyring() {
            Ok(_) => {
                println!("{}", "─".repeat(50).dimmed());
                println!(
                    "{} Profile '{}' saved successfully!",
                    "✅".bold(),
                    name.bold().green()
                );
                println!(
                    "   You can now use it with: {}",
                    format!("--profile {}", name).dimmed().italic()
                );
                println!("{}", "─".repeat(50).dimmed());
            }
            Err(e) => {
                println!(
                    "{} Failed to save profile to keyring: {}",
                    "❌".bold(),
                    e.to_string().red()
                );
            }
        },
        Err(e) => {
            println!("{} Failed to create profile: {}", "❌".bold(), e.red());
        }
    }
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
    /// 1. Gets the profile name (from args or prompt)
    /// 2. Gets credentials (from args or interactive prompt)
    /// 3. Creates and stores the profile in the system keyring
    fn process(self, _client: &BaseClient) {
        match self {
            AuthSubCommand::Set { name, url, token } => {
                let profile_name = match get_profile_name(name) {
                    Ok(name) => name,
                    Err(e) => {
                        println!(
                            "{} Failed to get profile name: {}",
                            "❌".bold(),
                            e.to_string().red()
                        );
                        return;
                    }
                };

                let (final_url, final_token) = match get_credentials(url, token) {
                    Ok(credentials) => credentials,
                    Err(e) => {
                        println!(
                            "{} Failed to get credentials: {}",
                            "❌".bold(),
                            e.to_string().red()
                        );
                        return;
                    }
                };

                create_and_store_profile(profile_name, final_url, final_token);
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
        format!("{}--{}", url, token)
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
