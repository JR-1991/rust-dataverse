use std::error::Error;

use colored::Colorize;
use dataverse::cli::admin::AdminSubCommand;
use dataverse::cli::auth::{prompt_for_credentials, AuthProfile, AuthSubCommand};
use structopt::StructOpt;

use dataverse::cli::base::Matcher;
use dataverse::cli::collection::CollectionSubCommand;
use dataverse::cli::dataset::DatasetSubCommand;
use dataverse::cli::file::FileSubCommand;
use dataverse::cli::info::InfoSubCommand;
use dataverse::client::BaseClient;
use dataverse::search_api::query::SearchQuery;

static HEADER: &str = r#"
--- Dataverse Command Line Interface (DVCLI) ---
"#;

#[derive(StructOpt, Debug)]
struct GlobalOpts {
    /// Profile name to use for configuration
    #[structopt(short, long)]
    profile: Option<String>,
}

#[derive(StructOpt, Debug)]
#[structopt(about = "CLI to interact with Dataverse")]
#[allow(clippy::upper_case_acronyms)]
struct CLI {
    #[structopt(flatten)]
    global: GlobalOpts,

    #[structopt(subcommand)]
    cmd: DVCLI,
}

#[derive(StructOpt, Debug)]
#[allow(clippy::upper_case_acronyms)]
enum DVCLI {
    Info(InfoSubCommand),
    Collection(CollectionSubCommand),
    Dataset(DatasetSubCommand),
    File(FileSubCommand),
    Search(SearchQuery),
    Admin(AdminSubCommand),
    Auth(AuthSubCommand),
}

fn main() {
    let cli = CLI::from_args();

    // This is a special case for the Auth command, which is used to set the profile
    // and does not require a Dataverse instance.
    if let DVCLI::Auth(cmd) = cli.cmd {
        let client = BaseClient::new("https://None", None).expect("Failed to create base client");
        cmd.process(&client);
        return;
    }

    let client = match cli.global.profile {
        Some(profile) => setup_client_from_keyring(&profile).expect("Failed to set up client."),
        None => {
            // Try environment variables first, if not available, prompt for interactive input
            match setup_client_from_env() {
                Ok(client) => client,
                Err(_) => setup_client_from_input().expect("Failed to set up client."),
            }
        }
    };

    if atty::is(atty::Stream::Stdout) {
        println!("{}", HEADER.bold());
    }

    // You can now access the profile via cli.global.profile
    match cli.cmd {
        DVCLI::Info(command) => command.process(&client),
        DVCLI::Collection(command) => command.process(&client),
        DVCLI::Dataset(command) => command.process(&client),
        DVCLI::File(command) => command.process(&client),
        DVCLI::Search(command) => command.process(&client),
        DVCLI::Admin(command) => command.process(&client),
        DVCLI::Auth(command) => command.process(&client),
    }
}

fn setup_client_from_keyring(name: &str) -> Result<BaseClient, Box<dyn Error>> {
    let auth_profile = AuthProfile::get_from_keyring(name)?;
    let client = BaseClient::new(
        auth_profile.get_url(),
        Some(&auth_profile.get_token().to_string()),
    )?;
    Ok(client)
}

fn setup_client_from_env() -> Result<BaseClient, Box<dyn Error>> {
    let (base_url, api_token) = extract_config_from_env();
    let client = BaseClient::new(&base_url, api_token.as_ref())?;
    Ok(client)
}

fn setup_client_from_input() -> Result<BaseClient, Box<dyn Error>> {
    let (base_url, api_token_str) = prompt_for_credentials()?;

    let api_token = if api_token_str.trim().is_empty() {
        None
    } else {
        Some(api_token_str)
    };

    let client = BaseClient::new(&base_url, api_token.as_ref())?;
    Ok(client)
}

// This function extracts the base URL and API token from the environment
// variables DVCLI_URL and DVCLI_TOKEN, respectively.
fn extract_config_from_env() -> (String, Option<String>) {
    let base_url = std::env::var("DVCLI_URL").ok();
    let api_token = std::env::var("DVCLI_TOKEN").ok();

    // If there is no base URL, return None
    if base_url.is_none() {
        panic!("Neither profile name nor base URL provided. Please set the DVCLI_URL environment variable or use a profile name with the --profile flag.");
    }

    (base_url.unwrap(), api_token)
}
