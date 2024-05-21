use std::path::PathBuf;
use structopt::StructOpt;

use crate::native_api::file::replace;
use crate::{client::BaseClient, native_api::dataset::upload::UploadBody};

use super::base::{evaluate_and_print_response, parse_file, Matcher};

#[derive(StructOpt, Debug)]
#[structopt(about = "Handle files of a Dataverse instance")]
pub enum FileSubCommand {
    #[structopt(about = "Replace a file")]
    Replace {
        #[structopt(help = "Path to the file to replace")]
        path: PathBuf,

        #[structopt(long, short, help = "Identifier of the of the file to replace")]
        id: String,

        #[structopt(
            long,
            short,
            help = "Path to the JSON/YAML file containing the file body"
        )]
        body: Option<PathBuf>,

        #[structopt(long, short, help = "Force the replacement of the file")]
        force: bool,
    },
}

impl Matcher for FileSubCommand {
    fn process(&self, client: &BaseClient) {
        match self {
            FileSubCommand::Replace {
                id,
                path,
                body,
                force,
            } => {
                let body = prepare_replace_body(body, force);
                let response =
                    replace::replace_file(client, id, &path.to_str().unwrap().to_string(), &body);

                evaluate_and_print_response(response);
            }
        };
    }
}

fn prepare_replace_body(body: &Option<PathBuf>, force: &bool) -> Option<UploadBody> {
    match body {
        Some(body) => {
            let mut body = parse_file::<_, UploadBody>(body).unwrap();
            if body.force_replace.is_none() {
                body.force_replace = Some(force.to_owned());
            }
            Some(body)
        }
        _ => None,
    }
}
