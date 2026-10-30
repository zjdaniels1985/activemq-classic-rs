use std::env;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

use activemq_classic_rs::config::AppConfig;

const USAGE: &str = "\
Usage: activemq-classic-rs [--config PATH] [--bind ADDRESS] [--data-dir PATH] [--in-memory]

  --config PATH     configuration file (default ./activemq-rust.toml if present)
  --bind ADDRESS    override the OpenWire listener address
  --data-dir PATH   override the Sled persistence directory
  --in-memory       disable persistence";

fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    match load_config(&args).and_then(run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("broker failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn load_config(args: &[String]) -> io::Result<AppConfig> {
    let mut config_path = None;
    let mut bind = None;
    let mut data_dir = None;
    let mut in_memory = false;
    let mut iter = args.iter();
    while let Some(argument) = iter.next() {
        let mut value = || {
            iter.next().cloned().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{argument} requires a value"),
                )
            })
        };
        match argument.as_str() {
            "--config" => config_path = Some(PathBuf::from(value()?)),
            "--bind" => bind = Some(value()?),
            "--data-dir" => data_dir = Some(value()?),
            "--in-memory" => in_memory = true,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {argument}\n{USAGE}"),
                ));
            }
        }
    }
    let default_path = PathBuf::from("activemq-rust.toml");
    let mut config = match config_path {
        Some(path) => AppConfig::from_file(&path)?,
        None if default_path.exists() => AppConfig::from_file(&default_path)?,
        None => AppConfig::default(),
    };
    if let Some(bind) = bind {
        config.protocols.openwire.enabled = true;
        config.protocols.openwire.bind = bind;
    }
    if let Some(path) = data_dir {
        config.persistence.backend = "sled".to_owned();
        config.persistence.path = path;
    }
    if in_memory {
        config.persistence.backend = "memory".to_owned();
    }
    config.validate()?;
    Ok(config)
}

fn run(config: AppConfig) -> io::Result<()> {
    activemq_classic_rs::logging::init(&config.logging)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("broker-io")
        .build()?;
    runtime.block_on(async move {
        let mut broker = activemq_classic_rs::runtime::start(config).await?;
        let result = tokio::select! {
            result = broker.wait() => result,
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutdown requested");
                Ok(())
            }
        };
        broker.stop();
        tracing::info!("broker stopped");
        result
    })
}
