fn main() {
    if let Err(e) = airlayer::cli::run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
