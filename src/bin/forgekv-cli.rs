use std::{env, error::Error, io, time::Duration};

use bytes::Bytes;
use forgekv::{
    client::Client,
    command::Command,
    config::Config,
    protocol::{ProtocolLimits, Response},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_env()?;
    let arguments: Vec<String> = env::args().skip(1).collect();
    let command = parse_arguments(&arguments)
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
    let mut client =
        Client::connect(&config.listen_address(), ProtocolLimits::from(&config)).await?;
    let response = client.execute(command).await?;
    print_response(response);
    Ok(())
}

fn parse_arguments(arguments: &[String]) -> Result<Command, String> {
    let name = arguments
        .first()
        .ok_or_else(|| usage("a command is required"))?
        .to_ascii_lowercase();
    match name.as_str() {
        "ping" => exact(arguments, 1).map(|_| Command::Ping),
        "set" => {
            exact(arguments, 3)?;
            Ok(Command::Set {
                key: bytes(&arguments[1]),
                value: bytes(&arguments[2]),
            })
        }
        "get" => one_key(arguments, |key| Command::Get { key }),
        "del" => one_key(arguments, |key| Command::Del { key }),
        "exists" => one_key(arguments, |key| Command::Exists { key }),
        "setex" => {
            exact(arguments, 4)?;
            let seconds = arguments[2]
                .parse::<u64>()
                .map_err(|_| usage("SETEX ttl must be a positive integer in seconds"))?;
            if seconds == 0 {
                return Err(usage("SETEX ttl must be greater than zero"));
            }
            Ok(Command::SetEx {
                key: bytes(&arguments[1]),
                ttl: Duration::from_secs(seconds),
                value: bytes(&arguments[3]),
            })
        }
        "ttl" => one_key(arguments, |key| Command::Ttl { key }),
        "persist" => one_key(arguments, |key| Command::Persist { key }),
        "info" => exact(arguments, 1).map(|_| Command::Info),
        "stats" => exact(arguments, 1).map(|_| Command::Stats),
        _ => Err(usage("unknown command")),
    }
}

fn one_key<F>(arguments: &[String], make: F) -> Result<Command, String>
where
    F: FnOnce(Bytes) -> Command,
{
    exact(arguments, 2)?;
    Ok(make(bytes(&arguments[1])))
}

fn exact(arguments: &[String], expected: usize) -> Result<(), String> {
    if arguments.len() == expected {
        Ok(())
    } else {
        Err(usage("invalid number of arguments"))
    }
}

fn bytes(value: &str) -> Bytes {
    Bytes::copy_from_slice(value.as_bytes())
}

fn usage(reason: &str) -> String {
    format!(
        "{reason}\nusage: forgekv-cli <ping|set KEY VALUE|get KEY|del KEY|exists KEY|setex KEY TTL_SECONDS VALUE|ttl KEY|persist KEY|info|stats>"
    )
}

fn print_response(response: Response) {
    match response {
        Response::Ok => println!("OK"),
        Response::NotFound => println!("NOT_FOUND"),
        Response::InvalidRequest(message) => eprintln!("INVALID_REQUEST: {message}"),
        Response::ServerError(message) => eprintln!("SERVER_ERROR: {message}"),
        Response::Pong => println!("PONG"),
        Response::Value(value) => println!("{}", String::from_utf8_lossy(&value)),
        Response::Integer(value) => println!("{value}"),
        Response::Info(fields) => {
            for (name, value) in fields {
                println!("{name}: {value}");
            }
        }
        Response::Stats(fields) => {
            for (name, value) in fields {
                println!("{name}: {value}");
            }
        }
        Response::Redirect(address) => eprintln!("REDIRECT: {address}"),
    }
}
