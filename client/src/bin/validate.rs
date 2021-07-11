fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let invalid = wayback_client::digest::validate_files_gz(&args[1], |path| {
        Some(path.file_stem().unwrap().to_string_lossy().to_string())
    })?;

    if invalid.is_empty() {
        println!("All files are valid");
    } else {
        for path in invalid {
            println!("{}", path.to_string_lossy());
        }
    }

    Ok(())
}
