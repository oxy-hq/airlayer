fn main() {
    if let Err(e) = o3::cli::run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
