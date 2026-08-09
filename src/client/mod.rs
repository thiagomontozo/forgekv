use std::time::Duration;

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
}

impl Client {
    pub async fn connect(address: &str, limits: ProtocolLimits) -> Result<Self, ProtocolError> {
        let stream = TcpStream::connect(address).await?;
        Ok(Self { stream, limits })
    }

    pub async fn execute(&mut self, command: Command) -> Result<Response, ProtocolError> {
        let frame = command.into_frame()?;
        write_frame(&mut self.stream, &frame, self.limits).await?;
        let response = read_frame(&mut self.stream, self.limits).await?.ok_or(
            ProtocolError::InvalidPayload("server closed the connection before responding"),
        )?;
        Response::from_frame(response)
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
        for command in commands {
            let encoded = command.into_frame()?.encode(self.limits)?;
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
