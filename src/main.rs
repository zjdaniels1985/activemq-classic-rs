use activemq_classic_rs::server::{ServerConfig, serve};
use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("Usage: activemq-classic-rs [--bind ADDRESS]");
        println!("Default bind address: 127.0.0.1:61616");
        return ExitCode::SUCCESS;
    }

    let mut config = ServerConfig::default();
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--bind" => match args.next() {
                Some(address) => config.bind_address = address,
                None => {
                    eprintln!("--bind requires an address");
                    return ExitCode::FAILURE;
                }
            },
            unknown => {
                eprintln!("unknown argument: {unknown}");
                eprintln!("Usage: activemq-classic-rs [--bind ADDRESS]");
                return ExitCode::FAILURE;
            }
        }
    }

    match serve(config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("broker failed: {error}");
            ExitCode::FAILURE
        }
    }
}
