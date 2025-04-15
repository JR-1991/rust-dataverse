//! Base functionality for the Dataverse CLI
//!
//! This module provides core utilities and traits used across the CLI including:
//! - Response handling and printing
//! - Argument parsing and validation
//! - File parsing for JSON/YAML configs
//! - Common traits for command processing

use std::error::Error;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use clap::ArgMatches;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::client::BaseClient;
use crate::response::Response;

/// Evaluates an API response and prints the result or error message
///
/// # Arguments
/// * `response` - The Result containing either a Response<T> or error string
///
/// # Type Parameters
/// * `T` - The type of data contained in a successful response
pub fn evaluate_and_print_response<T: Serialize>(response: Result<Response<T>, String>) {
    match response {
        Ok(response) => {
            response.print_result();
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }
}

/// Retrieves and parses a command line argument with validation
///
/// # Arguments
/// * `matches` - The ArgMatches containing parsed command line arguments
/// * `arg_name` - Name of the argument to retrieve
///
/// # Type Parameters
/// * `U` - The string-like type of the raw argument value
/// * `T` - The target type to parse the argument into
///
/// # Panics
/// * If the argument is missing or invalid
pub fn get_argument<U, T>(matches: &ArgMatches, arg_name: &str) -> T
where
    U: AsRef<str> + Send + Sync + Clone + 'static,
    T: FromStr,
    T::Err: std::fmt::Debug,
{
    let value = matches
        .get_one::<U>(arg_name)
        .unwrap_or_else(|| panic!("{} is required.", arg_name))
        .as_ref()
        .parse::<T>()
        .unwrap_or_else(|_| panic!("{} is invalid.", arg_name));

    value
}

/// Parses a JSON or YAML file into the specified type
///
/// # Arguments
/// * `path` - Path to the file to parse
///
/// # Type Parameters
/// * `P` - The path-like type for the file path
/// * `T` - The target type to deserialize into
///
/// # Returns
/// * `Ok(T)` - Successfully parsed file contents
/// * `Err` - File reading or parsing error
pub fn parse_file<P, T>(path: P) -> Result<T, Box<dyn Error>>
where
    T: DeserializeOwned,
    P: AsRef<Path>,
{
    let content = fs::read_to_string(path)?;

    if let Ok(content) = serde_json::from_str(&content) {
        Ok(content)
    } else if let Ok(content) = serde_yaml::from_str(&content) {
        Ok(content)
    } else {
        Err("Failed to parse the file as either JSON or YAML".into())
    }
}

/// Trait for processing CLI subcommands
///
/// Implementors define how to handle their specific subcommand variant
/// using the provided API client.
pub trait Matcher {
    /// Process this subcommand using the given client
    ///
    /// # Arguments
    /// * `client` - The BaseClient for making API requests
    fn process(self, client: &BaseClient);
}
