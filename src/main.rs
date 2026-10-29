use activemq_classic_rs::config::AppConfig;
use activemq_classic_rs::server::{ServerConfig, serve};
use std::env;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("Usage: activemq-classic-rs [--config PATH] [--bind ADDRESS] [--data-dir PATH]");
        println!("Default config: ./activemq-rust.toml (if present)");
        println!("Default bind address: 127.0.0.1:61616");
        println!("Default data directory: ./data/sled");
        return ExitCode::SUCCESS;
    }

    match parse_args(&args).and_then(start_broker) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("broker failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn start_broker(
    (config_path, bind, data_dir): (Option<PathBuf>, Option<String>, Option<PathBuf>),
) -> io::Result<()> {
    let app_config = match config_path {
        Some(path) => AppConfig::from_file(&path)?,
        None if PathBuf::from("activemq-rust.toml").exists() => {
            AppConfig::from_file(PathBuf::from("activemq-rust.toml").as_path())?
        }
        None => AppConfig::default(),
    };
    let mut config = ServerConfig::try_from(app_config)?;
    if let Some(bind) = bind {
        config.bind_address = bind;
    }
    if let Some(data_dir) = data_dir {
        config.store_path = data_dir;
    }

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(io::Error::other)?;
    serve(config)
}

fn parse_args(args: &[String]) -> io::Result<(Option<PathBuf>, Option<String>, Option<PathBuf>)> {
    let mut config = None;
    let mut bind = None;
    let mut data_dir = None;
    let mut args = args.iter();
    while let Some(argument) = args.next() {
        let value = args.next();
        match (argument.as_str(), value) {
            ("--config", Some(path)) => config = Some(PathBuf::from(path)),
            ("--bind", Some(address)) => bind = Some(address.clone()),
            ("--data-dir", Some(path)) => data_dir = Some(PathBuf::from(path)),
            ("--config" | "--bind" | "--data-dir", None) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{argument} requires a value"),
                ));
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {argument}"),
                ));
            }
        }
    }
    Ok((config, bind, data_dir))
}
