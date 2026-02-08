fn main() {
    let args: Vec<String> = std::env::args().collect();
    if let Err(err) = tibia::run(&args) {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}
