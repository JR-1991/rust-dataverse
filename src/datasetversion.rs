use std::fmt::Display;
use std::str::FromStr;

/// Represents different versions of a dataset.
#[derive(Debug, Clone, PartialEq)]
pub enum DatasetVersion {
    /// The latest version of the dataset.
    Latest,
    /// The latest published version of the dataset.
    LatestPublished,
    /// The draft version of the dataset.
    Draft,
    /// A specific version of the dataset, represented as a string.
    Version(String),
}

impl FromStr for DatasetVersion {
    type Err = String;

    /// Converts a string slice to a `DatasetVersion`.
    ///
    /// # Arguments
    ///
    /// * `s` - A string slice that holds the version information.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    /// - `Ok(DatasetVersion)` if the string matches one of the predefined versions.
    /// - `Ok(DatasetVersion::Version(String))` if the string does not match predefined versions.
    /// - `Err(String)` if an error occurs.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "latest" => Ok(DatasetVersion::Latest),
            "draft" => Ok(DatasetVersion::Draft),
            "latest-published" => Ok(DatasetVersion::LatestPublished),
            _ => Ok(DatasetVersion::Version(s.to_string())),
        }
    }
}

impl Display for DatasetVersion {
    /// Formats the `DatasetVersion` for display.
    ///
    /// # Arguments
    ///
    /// * `f` - A mutable reference to a `Formatter`.
    ///
    /// # Returns
    ///
    /// A `Result` which is:
    /// - `Ok(())` if the formatting is successful.
    /// - `Err(std::fmt::Error)` if an error occurs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatasetVersion::Latest => write!(f, ":latest"),
            DatasetVersion::Draft => write!(f, ":draft"),
            DatasetVersion::LatestPublished => write!(f, ":latest-published"),
            DatasetVersion::Version(v) => write!(f, "{v}"),
        }
    }
}

/// Determines the version of the dataset to download.
///
/// # Arguments
///
/// * `version` - An optional `DatasetVersion` specifying the version of the dataset to download.
/// * `has_api_token` - A boolean indicating whether the client has an API token.
///
/// # Returns
///
/// A `DatasetVersion` instance representing the version of the dataset to download.
pub fn determine_version(version: &Option<DatasetVersion>, has_api_token: bool) -> DatasetVersion {
    match version {
        Some(v) => v.clone(),
        None => {
            if has_api_token {
                DatasetVersion::Draft
            } else {
                DatasetVersion::Latest
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn from_str_converts_latest() {
        let version = DatasetVersion::from_str("latest").unwrap();
        assert_eq!(version, DatasetVersion::Latest);
    }

    #[test]
    fn from_str_converts_draft() {
        let version = DatasetVersion::from_str("draft").unwrap();
        assert_eq!(version, DatasetVersion::Draft);
    }

    #[test]
    fn from_str_converts_latest_published() {
        let version = DatasetVersion::from_str("latest-published").unwrap();
        assert_eq!(version, DatasetVersion::LatestPublished);
    }

    #[test]
    fn from_str_converts_specific_version() {
        let version = DatasetVersion::from_str("v1.0").unwrap();
        assert_eq!(version, DatasetVersion::Version("v1.0".to_string()));
    }

    #[test]
    fn display_formats_latest() {
        let version = DatasetVersion::Latest;
        assert_eq!(version.to_string(), ":latest");
    }

    #[test]
    fn display_formats_draft() {
        let version = DatasetVersion::Draft;
        assert_eq!(version.to_string(), ":draft");
    }

    #[test]
    fn display_formats_latest_published() {
        let version = DatasetVersion::LatestPublished;
        assert_eq!(version.to_string(), ":latest-published");
    }

    #[test]
    fn display_formats_specific_version() {
        let version = DatasetVersion::Version("v1.0".to_string());
        assert_eq!(version.to_string(), "v1.0");
    }

    #[test]
    fn determine_version_returns_provided_version() {
        let version = Some(DatasetVersion::Draft);
        let result = determine_version(&version, true);
        assert_eq!(result, DatasetVersion::Draft);
    }

    #[test]
    fn determine_version_returns_draft_if_no_version_and_has_api_token() {
        let result = determine_version(&None, true);
        assert_eq!(result, DatasetVersion::Draft);
    }

    #[test]
    fn determine_version_returns_latest_if_no_version_and_no_api_token() {
        let result = determine_version(&None, false);
        assert_eq!(result, DatasetVersion::Latest);
    }
}
