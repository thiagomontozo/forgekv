use std::{collections::HashSet, time::Duration};

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::{
    command::Command,
    error::ProtocolError,
    protocol::{read_frame, write_frame, ProtocolLimits, Response},
};

#[derive(Debug)]
pub struct Client {
    stream: TcpStream,
    limits: ProtocolLimits,
    address: String,
    max_redirects: usize,
}

impl Client {
    pub async fn connect(address: &str, limits: ProtocolLimits) -> Result<Self, ProtocolError> {
        Self::connect_with_max_redirects(address, limits, 5).await
    }

    pub async fn connect_with_max_redirects(
        address: &str,
        limits: ProtocolLimits,
        max_redirects: usize,
    ) -> Result<Self, ProtocolError> {
        let stream = TcpStream::connect(address).await?;
        Ok(Self {
            stream,
            limits,
            address: address.to_owned(),
            max_redirects,
        })
    }

    pub async fn execute(&mut self, command: Command) -> Result<Response, ProtocolError> {
        let frame = command.into_frame()?;
        let mut visited = HashSet::from([self.address.clone()]);
        let mut redirects = 0usize;
        loop {
            write_frame(&mut self.stream, &frame, self.limits).await?;
            let response = read_frame(&mut self.stream, self.limits).await?.ok_or(
                ProtocolError::InvalidPayload("server closed the connection before responding"),
            )?;
            match Response::from_frame(response)? {
                Response::Redirect(address) => {
                    if redirects >= self.max_redirects {
                        return Err(ProtocolError::RedirectLimitExceeded);
                    }
                    if !visited.insert(address.clone()) {
                        return Err(ProtocolError::RedirectLoop(address));
                    }
                    validate_redirect_address(&address)?;
                    self.stream = TcpStream::connect(&address).await?;
                    self.address = address;
                    redirects = redirects
                        .checked_add(1)
                        .ok_or(ProtocolError::IntegerOverflow)?;
                }
                response => return Ok(response),
            }
        }
    }

    pub async fn execute_pipeline(
        &mut self,
        commands: Vec<Command>,
    ) -> Result<Vec<Response>, ProtocolError> {
        const MAX_PIPELINE_COMMANDS: usize = 1_024;
        if commands.len() > MAX_PIPELINE_COMMANDS {
            return Err(ProtocolError::InvalidPayload(
                "pipeline exceeds 1024 commands",
            ));
        }
        let response_count = commands.len();
        for command in &commands {
            let encoded = command.clone().into_frame()?.encode(self.limits)?;
            self.stream.write_all(&encoded).await?;
        }
        self.stream.flush().await?;
        let mut responses = Vec::with_capacity(response_count);
        for _ in 0..response_count {
            let frame = read_frame(&mut self.stream, self.limits).await?.ok_or(
                ProtocolError::InvalidPayload("server closed during pipeline response"),
            )?;
            responses.push(Response::from_frame(frame)?);
        }
        for (command, response) in commands.into_iter().zip(&mut responses) {
            if let Response::Redirect(address) = response {
                if self.max_redirects == 0 {
                    return Err(ProtocolError::RedirectLimitExceeded);
                }
                validate_redirect_address(address)?;
                let mut redirected = Self::connect_with_max_redirects(
                    address.as_str(),
                    self.limits,
                    self.max_redirects - 1,
                )
                .await?;
                *response = redirected.execute(command).await?;
            }
        }
        Ok(responses)
    }

    pub async fn ping(&mut self) -> Result<Response, ProtocolError> {
        self.execute(Command::Ping).await
    }

    pub async fn set(&mut self, key: Bytes, value: Bytes) -> Result<Response, ProtocolError> {
        self.execute(Command::Set { key, value }).await
    }

    pub async fn get(&mut self, key: Bytes) -> Result<Response, ProtocolError> {
        self.execute(Command::Get { key }).await
    }

    pub async fn delete(&mut self, key: Bytes) -> Result<Response, ProtocolError> {
        self.execute(Command::Del { key }).await
    }

    pub async fn exists(&mut self, key: Bytes) -> Result<Response, ProtocolError> {
        self.execute(Command::Exists { key }).await
    }

    pub async fn set_ex(
        &mut self,
        key: Bytes,
        ttl: Duration,
        value: Bytes,
    ) -> Result<Response, ProtocolError> {
        self.execute(Command::SetEx { key, ttl, value }).await
    }

    pub async fn ttl(&mut self, key: Bytes) -> Result<Response, ProtocolError> {
        self.execute(Command::Ttl { key }).await
    }

    pub async fn persist(&mut self, key: Bytes) -> Result<Response, ProtocolError> {
        self.execute(Command::Persist { key }).await
    }

    pub async fn info(&mut self) -> Result<Response, ProtocolError> {
        self.execute(Command::Info).await
    }

    pub async fn stats(&mut self) -> Result<Response, ProtocolError> {
        self.execute(Command::Stats).await
    }
}

fn validate_redirect_address(address: &str) -> Result<(), ProtocolError> {
    let valid_port = address
        .rsplit_once(':')
        .filter(|(host, _)| !host.is_empty())
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .is_some_and(|port| port > 0);
    if address.len() > 255
        || !address.is_ascii()
        || address.bytes().any(|byte| byte.is_ascii_whitespace())
        || !valid_port
    {
        return Err(ProtocolError::InvalidPayload(
            "redirect address must be a valid host:port",
        ));
    }
    Ok(())
}
