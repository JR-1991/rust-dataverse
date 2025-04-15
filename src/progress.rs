use indicatif::{ProgressBar, ProgressStyle};

/// Creates a progress bar or spinner for a file.
///
/// # Arguments
/// * `file_size` - The size of the file in bytes.
/// * `offset` - An optional offset to start the progress bar from.
/// * `name` - The name of the file.
///
/// # Returns
/// A `ProgressBar` instance.
pub fn setup_progress_log(
    file_size: u64,
    offset: Option<u64>,
    name: &str,
) -> ProgressBar {
    if file_size == 0 {
        spinner()
    } else {
        progress_bar(file_size, offset, name)
    }
}

/// Creates a progress bar for a file with a known size.
///
/// # Arguments
/// * `file_size` - The size of the file in bytes.
/// * `offset` - An optional offset to start the progress bar from.
/// * `name` - The name of the file.
///
/// # Returns
/// A `ProgressBar` instance.
fn progress_bar(file_size: u64, offset: Option<u64>, name: &str) -> ProgressBar {
    let pb = ProgressBar::new(file_size);

    if let Some(offset) = offset {
        pb.inc(offset);
    }

    pb.set_style(ProgressStyle::default_bar()
        .template(&(name.to_owned() + " {bar:40.cyan} {percent:.cyan}% | {bytes}/{total_bytes} ({eta})\n"))
        .expect("Could not set progress bar style")
        .progress_chars("=>-"));

    pb
}

/// Creates a spinner progress bar for a file with an unknown size.
///
/// # Returns
/// A `ProgressBar` instance.
fn spinner() -> ProgressBar {
    let pb = ProgressBar::new_spinner();

    pb.set_style(ProgressStyle::default_spinner()
        .template("{spinner:.cyan} Uploaded {bytes}")
        .expect("Error setting progress style")
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠏"));

    pb
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setup_progress_log() {
        let pb = setup_progress_log(100, None, "test");
        assert_eq!(pb.length(), Some(100));
    }

    #[test]
    fn test_progress_bar() {
        let pb = progress_bar(100, None, "test");
        assert_eq!(pb.length(), Some(100));
    }

    #[test]
    fn test_spinner() {
        let pb = spinner();
        assert_eq!(pb.length(), None);
    }
}