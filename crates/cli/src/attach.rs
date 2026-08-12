use std::io::{self, BufRead, BufReader, Write};
use std::thread;
use std::time::Duration;

use api::{EventEnvelope, RunEvent, RunState};

use crate::client::{Client, ClientError};

const RECONNECT_DELAY: Duration = Duration::from_millis(250);

pub(crate) fn attach(
    client: &Client,
    run_id: &str,
    output: &mut impl Write,
) -> Result<(), AttachError> {
    let mut last_event_id = None;
    let mut connected = false;
    loop {
        let response = match client.events(run_id, last_event_id) {
            Ok(response) => response,
            Err(error) if connected && error.is_transport() => {
                thread::sleep(RECONNECT_DELAY);
                continue;
            }
            Err(error) => return Err(AttachError::Client(error)),
        };
        connected = true;
        let mut reader = BufReader::new(response.into_body().into_reader());
        match consume(&mut reader, &mut last_event_id, output)? {
            StreamEnd::Terminal => return Ok(()),
            StreamEnd::Disconnected => thread::sleep(RECONNECT_DELAY),
        }
    }
}

fn consume(
    reader: &mut impl BufRead,
    last_event_id: &mut Option<u64>,
    output: &mut impl Write,
) -> Result<StreamEnd, AttachError> {
    let mut frame = Frame::default();
    loop {
        let mut line = String::new();
        let read = match reader.read_line(&mut line) {
            Ok(read) => read,
            Err(_) => return Ok(StreamEnd::Disconnected),
        };
        if read == 0 {
            return Ok(StreamEnd::Disconnected);
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            if let Some(event) = frame.take_event(*last_event_id)? {
                *last_event_id = Some(event.seq);
                serde_json::to_writer(&mut *output, &event)?;
                output.write_all(b"\n")?;
                output.flush()?;
                if is_terminal(&event.event) {
                    return Ok(StreamEnd::Terminal);
                }
            }
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "id" => frame.id = Some(value.to_owned()),
            "data" => frame.data.push(value.to_owned()),
            _ => {}
        }
    }
}

fn is_terminal(event: &RunEvent) -> bool {
    matches!(
        event,
        RunEvent::StateChanged {
            state: RunState::Done | RunState::Failed | RunState::Cancelled
        }
    )
}

#[derive(Default)]
struct Frame {
    id: Option<String>,
    data: Vec<String>,
}

impl Frame {
    fn take_event(
        &mut self,
        last_event_id: Option<u64>,
    ) -> Result<Option<EventEnvelope>, AttachError> {
        if self.data.is_empty() {
            self.id = None;
            return Ok(None);
        }
        let id = self
            .id
            .take()
            .ok_or_else(|| AttachError::Stream("event is missing its SSE id".into()))?
            .parse::<u64>()
            .map_err(|_| AttachError::Stream("event has a non-numeric SSE id".into()))?;
        let data = std::mem::take(&mut self.data).join("\n");
        let event: EventEnvelope = serde_json::from_str(&data)
            .map_err(|error| AttachError::Stream(format!("invalid event data: {error}")))?;
        if event.seq != id {
            return Err(AttachError::Stream(format!(
                "SSE id {id} does not match event sequence {}",
                event.seq
            )));
        }
        let expected = last_event_id.map_or(0, |seq| seq.saturating_add(1));
        if event.seq < expected {
            return Ok(None);
        }
        if event.seq != expected {
            return Err(AttachError::Stream(format!(
                "event sequence jumped from {} to {}",
                last_event_id.map_or_else(|| "start".into(), |seq| seq.to_string()),
                event.seq
            )));
        }
        Ok(Some(event))
    }
}

enum StreamEnd {
    Terminal,
    Disconnected,
}

#[derive(Debug)]
pub(crate) enum AttachError {
    Client(ClientError),
    Stream(String),
    Output(io::Error),
    Json(serde_json::Error),
}

impl From<io::Error> for AttachError {
    fn from(error: io::Error) -> Self {
        Self::Output(error)
    }
}

impl From<serde_json::Error> for AttachError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl std::fmt::Display for AttachError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client(error) => error.fmt(formatter),
            Self::Stream(message) => formatter.write_str(message),
            Self::Output(error) => write!(formatter, "could not print event: {error}"),
            Self::Json(error) => write!(formatter, "could not encode event: {error}"),
        }
    }
}

impl std::error::Error for AttachError {}
