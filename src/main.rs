use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing::info;

mod auth;
mod config;
mod protocol;
mod rate_limit;
mod server;
mod session;
mod transport;
mod users;

use config::Config;

#[derive(Parser)]
#[command(name = "nautilus", about = "Bridge Socket.IO/WebSocket clients to remote CLI sessions")]
struct Cli {
    #[arg(short, long, default_value = "config.toml", global = true)]
    config: PathBuf,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the nautilus server
    Serve {
        #[arg(short, long)]
        port: Option<u16>,
    },
    /// Generate a JWT token for a user
    Token {
        #[arg(short, long)]
        user: String,
        #[arg(short, long, default_value = "admin")]
        role: String,
        #[arg(short, long)]
        expiry: Option<u64>,
    },
    /// Add a user to the database
    AddUser {
        #[arg(short, long)]
        user: String,
        #[arg(short, long)]
        password: String,
        #[arg(short, long, default_value = "admin")]
        role: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;

    match cli.command {
        Some(Commands::Token { user, role, expiry }) => {
            let expiry_hours = expiry.unwrap_or(config.auth.token_expiry_hours);
            let token = auth::claims::Claims::encode(
                &user,
                &role,
                expiry_hours,
                &config.auth.jwt_secret,
            )?;
            println!("{}", token);
            Ok(())
        }
        Some(Commands::AddUser {
            user,
            password,
            role,
        }) => {
            let store = users::UserStore::open(&config.auth.db_path)?;
            store.create_user(&user, &password, &role)?;
            println!("User '{}' created with role '{}'", user, role);
            Ok(())
        }
        Some(Commands::Serve { port }) => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "nautilus=info,tower_http=info".into()),
                )
                .init();

            let mut config = config;
            if let Some(p) = port {
                config.server.port = p;
            }

            info!(
                "Starting nautilus on {}:{}",
                config.server.host, config.server.port
            );

            server::run(config).await
        }
        None => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "nautilus=info,tower_http=info".into()),
                )
                .init();

            info!(
                "Starting nautilus on {}:{}",
                config.server.host, config.server.port
            );

            server::run(config).await
        }
    }
}
