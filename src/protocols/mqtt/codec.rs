//! Bounded MQTT 3.x packet framing and payload decoding.

use std::io;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(super) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[derive(Debug)]
pub(super) struct Packet {
    pub header: u8,
    pub body: Vec<u8>,
}

pub(super) async fn read<R: AsyncRead + Unpin>(reader: &mut R, max: usize) -> io::Result<Packet> {
    let header = reader.read_u8().await?;
    let mut length = 0usize;
    let mut bytes = 1;
    for shift in [0, 7, 14, 21] {
        let digit = reader.read_u8().await?;
        bytes += 1;
        length |= usize::from(digit & 127) << shift;
        if length.saturating_add(bytes) > max {
            return Err(invalid("MQTT frame exceeds max_frame_size"));
        }
        if digit & 128 == 0 {
            validate_header(header)?;
            let mut body = vec![0; length];
            reader.read_exact(&mut body).await?;
            return Ok(Packet { header, body });
        }
    }
    Err(invalid("MQTT remaining length exceeds four bytes"))
}

fn validate_header(header: u8) -> io::Result<()> {
    let kind = header >> 4;
    let flags = header & 15;
    let valid = match kind {
        3 => (flags >> 1) & 3 != 3 && !((flags >> 1) & 3 == 0 && flags & 8 != 0),
        6 | 8 | 10 => flags == 2,
        1..=14 => flags == 0,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid("invalid MQTT fixed header"))
    }
}

pub(super) async fn write<W: AsyncWrite + Unpin>(
    writer: &mut W,
    header: u8,
    body: &[u8],
    max: usize,
) -> io::Result<()> {
    let mut prefix = vec![header];
    let mut length = body.len();
    if length > 268_435_455 {
        return Err(invalid("MQTT packet too large"));
    }
    loop {
        let digit = (length % 128) as u8;
        length /= 128;
        prefix.push(digit | if length > 0 { 128 } else { 0 });
        if length == 0 {
            break;
        }
    }
    if body.len().saturating_add(prefix.len()) > max {
        return Err(invalid("MQTT frame exceeds max_frame_size"));
    }
    writer.write_all(&prefix).await?;
    writer.write_all(body).await?;
    writer.flush().await
}

pub(super) struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }
    pub fn byte(&mut self) -> io::Result<u8> {
        let (&first, rest) = self
            .0
            .split_first()
            .ok_or_else(|| invalid("truncated MQTT payload"))?;
        self.0 = rest;
        Ok(first)
    }
    pub fn word(&mut self) -> io::Result<u16> {
        Ok(u16::from_be_bytes([self.byte()?, self.byte()?]))
    }
    pub fn id(&mut self) -> io::Result<u16> {
        let id = self.word()?;
        if id == 0 {
            return Err(invalid("zero packet identifier"));
        }
        Ok(id)
    }
    pub fn binary(&mut self) -> io::Result<Vec<u8>> {
        let length = usize::from(self.word()?);
        if self.0.len() < length {
            return Err(invalid("truncated MQTT string"));
        }
        let (value, rest) = self.0.split_at(length);
        self.0 = rest;
        Ok(value.to_vec())
    }
    pub fn string(&mut self) -> io::Result<String> {
        let value = String::from_utf8(self.binary()?).map_err(|_| invalid("invalid MQTT UTF-8"))?;
        if value.chars().any(|c| {
            c == '\0' || ('\u{fdd0}'..='\u{fdef}').contains(&c) || (c as u32 & 0xffff) >= 0xfffe
        }) {
            return Err(invalid("forbidden MQTT UTF-8 character"));
        }
        Ok(value)
    }
    pub fn rest(&mut self) -> Vec<u8> {
        let value = self.0.to_vec();
        self.0 = &[];
        value
    }
    pub fn empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn end(&self) -> io::Result<()> {
        if self.empty() {
            Ok(())
        } else {
            Err(invalid("unexpected MQTT payload bytes"))
        }
    }
}

pub(super) fn string(bytes: &mut Vec<u8>, value: &str) -> io::Result<()> {
    let length = u16::try_from(value.len()).map_err(|_| invalid("MQTT string too long"))?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Publish {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}

impl Publish {
    pub fn decode(packet: &Packet) -> io::Result<(Self, u16)> {
        let mut cursor = Cursor::new(&packet.body);
        let topic = cursor.string()?;
        validate_topic(&topic, false)?;
        let qos = (packet.header >> 1) & 3;
        let id = if qos > 0 { cursor.id()? } else { 0 };
        Ok((
            Self {
                topic,
                qos,
                retain: packet.header & 1 != 0,
                payload: cursor.rest(),
            },
            id,
        ))
    }
}

pub(super) struct Connect {
    pub version: u8,
    pub clean: bool,
    pub keep_alive: u16,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub will: Option<Publish>,
}

impl Connect {
    /// A protocol mismatch is checked separately, so it can return CONNACK 1.
    pub fn decode(body: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(body);
        cursor.string()?;
        let version = cursor.byte()?;
        let flags = cursor.byte()?;
        let will_qos = (flags >> 3) & 3;
        let has_will = flags & 4 != 0;
        if flags & 1 != 0
            || will_qos == 3
            || (!has_will && flags & 0x38 != 0)
            || (version == 4 && flags & 0x40 != 0 && flags & 0x80 == 0)
        {
            return Err(invalid("invalid MQTT CONNECT flags"));
        }
        let keep_alive = cursor.word()?;
        let client_id = cursor.string()?;
        let will = if has_will {
            let topic = cursor.string()?;
            validate_topic(&topic, false)?;
            Some(Publish {
                topic,
                payload: cursor.binary()?,
                qos: will_qos,
                retain: flags & 0x20 != 0,
            })
        } else {
            None
        };
        let username = if flags & 0x80 != 0 {
            Some(cursor.string()?)
        } else {
            None
        };
        let password = if flags & 0x40 != 0 {
            Some(
                String::from_utf8(cursor.binary()?)
                    .map_err(|_| invalid("password is not UTF-8"))?,
            )
        } else {
            None
        };
        cursor.end()?;
        Ok(Self {
            version,
            clean: flags & 2 != 0,
            keep_alive,
            client_id,
            username,
            password,
            will,
        })
    }
}

pub(super) fn validate_topic(topic: &str, filter: bool) -> io::Result<()> {
    if topic.is_empty() || topic.len() > 65535 || topic.contains('\0') {
        return Err(invalid("invalid MQTT topic length"));
    }
    let levels: Vec<_> = topic.split('/').collect();
    for (index, level) in levels.iter().enumerate() {
        if level.contains('#') && (!filter || *level != "#" || index + 1 != levels.len())
            || level.contains('+') && (!filter || *level != "+")
        {
            return Err(invalid("invalid MQTT topic wildcard"));
        }
    }
    Ok(())
}

pub(super) fn to_activemq(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '/' => '.',
            '.' => '/',
            '+' => '*',
            '*' => '+',
            '#' => '>',
            '>' => '#',
            c => c,
        })
        .collect()
}

pub(super) fn to_mqtt(name: &str) -> String {
    // ActiveMQ's transformation swaps character pairs and is its own inverse.
    to_activemq(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_mapping_matches_activemq() {
        assert_eq!(to_activemq("a.b/c/+/#/*/>"), "a/b.c.*.>.+.#");
        assert_eq!(to_mqtt(&to_activemq("a.b/c/+/#/*/>")), "a.b/c/+/#/*/>");
        for valid in ["#", "+", "a/+/#", "/", "a//b", "a.b"] {
            assert!(validate_topic(valid, true).is_ok());
        }
        for bad in ["", "a#", "a/#/b", "a+", "a/+b"] {
            assert!(validate_topic(bad, true).is_err());
        }
    }

    #[tokio::test]
    async fn rejects_oversized_and_malformed_lengths_before_allocation() {
        for bytes in [
            &[0x30, 0xff, 0xff, 0xff, 0xff][..],
            &[0x30, 127],
            &[0x80, 0],
        ] {
            assert!(read(&mut &bytes[..], 64).await.is_err());
        }
        let body = vec![1; 16_384];
        let mut encoded = Vec::new();
        write(&mut encoded, 0x30, &body, 20_000).await.unwrap();
        assert_eq!(read(&mut &encoded[..], 20_000).await.unwrap().body, body);
    }
}
