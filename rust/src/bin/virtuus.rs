use clap::Parser;

#[derive(Parser)]
#[command(name = "virtuus", version)]
struct Cli {}

fn main() {
    Cli::parse();
}
