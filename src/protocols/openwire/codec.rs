//! Loose-encoding OpenWire v1–v12 command codecs.
use std::io::{self, Cursor, Read};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenWireFrame {
    pub command_type: u8,
    pub payload: Vec<u8>,
}

impl OpenWireFrame {
    pub fn read_from(reader: &mut impl Read, max_frame_size: usize) -> io::Result<Self> {
        let size = read_u32(reader)? as usize;
        if size == 0 || size > max_frame_size {
            return Err(invalid("invalid or oversized OpenWire frame"));
        }
        let mut bytes = vec![0; size];
        reader.read_exact(&mut bytes)?;
        Ok(Self {
            command_type: bytes[0],
            payload: bytes[1..].to_vec(),
        })
    }

    pub fn write_to(&self, writer: &mut impl io::Write, max_frame_size: usize) -> io::Result<()> {
        let size = self
            .payload
            .len()
            .checked_add(1)
            .filter(|n| *n <= max_frame_size && *n <= i32::MAX as usize)
            .ok_or_else(|| invalid("OpenWire frame exceeds maximum size"))?;
        writer.write_all(&(size as u32).to_be_bytes())?;
        writer.write_all(&[self.command_type])?;
        writer.write_all(&self.payload)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct WireId {
    pub connection: String,
    pub session: i64,
    pub value: i64,
}

impl WireId {
    pub fn display(&self) -> String {
        format!("{}:{}:{}", self.connection, self.session, self.value)
    }
}

#[derive(Clone, Debug)]
pub struct CommandHeader {
    pub id: i32,
    pub response_required: bool,
}

#[derive(Clone, Debug)]
pub struct IncomingMessage {
    pub header: CommandHeader,
    pub tx: Option<crate::core::TxId>,
    pub producer_id: Option<WireId>,
    pub destination: Option<WireDestination>,
    pub message_id: String,
    pub correlation_id: Option<String>,
    pub reply_to: Option<WireDestination>,
    pub group_id: Option<String>,
    pub group_sequence: i32,
    pub persistent: bool,
    pub expiration: i64,
    pub timestamp: i64,
    pub priority: u8,
    pub message_type: Option<String>,
    pub properties: Vec<u8>,
    pub body: Vec<u8>,
    pub compressed: bool,
    pub broker_path: Vec<String>,
    pub command_type: u8,
    pub advisory: Option<crate::core::AdvisoryData>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireDestination {
    pub kind: u8,
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct ConsumerInfo {
    pub header: CommandHeader,
    pub id: WireId,
    pub browser: bool,
    pub destination: WireDestination,
    pub prefetch: i32,
    pub selector: Option<String>,
    pub subscription_name: Option<String>,
    pub no_local: bool,
    pub exclusive: bool,
    pub retroactive: bool,
    pub priority: i8,
    pub network: bool,
    pub broker_path: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ProducerInfo {
    pub header: CommandHeader,
    pub id: WireId,
    pub destination: Option<WireDestination>,
    pub window_size: i32,
}

#[derive(Clone, Debug)]
pub struct AckInfo {
    pub header: CommandHeader,
    pub consumer: WireId,
    pub ack_type: u8,
    pub message_id: Option<String>,
    pub count: i32,
    pub tx: Option<crate::core::TxId>,
}

pub struct Decoder<'a> {
    input: Cursor<&'a [u8]>,
}

impl<'a> Decoder<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            input: Cursor::new(input),
        }
    }
    pub fn position(&self) -> u64 {
        self.input.position()
    }
    pub fn u8(&mut self) -> io::Result<u8> {
        read_u8(&mut self.input)
    }
    pub fn i8(&mut self) -> io::Result<i8> {
        Ok(self.u8()? as i8)
    }
    pub fn bool(&mut self) -> io::Result<bool> {
        Ok(self.u8()? != 0)
    }
    pub fn i16(&mut self) -> io::Result<i16> {
        let position = self.position();
        Ok(i16::from_be_bytes(
            read_array(&mut self.input).map_err(|error| positioned_error(error, position))?,
        ))
    }
    pub fn u16(&mut self) -> io::Result<u16> {
        let position = self.position();
        Ok(u16::from_be_bytes(
            read_array(&mut self.input).map_err(|error| positioned_error(error, position))?,
        ))
    }
    pub fn i32(&mut self) -> io::Result<i32> {
        let position = self.position();
        Ok(i32::from_be_bytes(
            read_array(&mut self.input).map_err(|error| positioned_error(error, position))?,
        ))
    }
    pub fn i64(&mut self) -> io::Result<i64> {
        let position = self.position();
        Ok(i64::from_be_bytes(
            read_array(&mut self.input).map_err(|error| positioned_error(error, position))?,
        ))
    }
    pub fn string(&mut self) -> io::Result<Option<String>> {
        if !self.bool()? {
            return Ok(None);
        }
        let length = usize::from(self.u16()?);
        let mut bytes = vec![0; length];
        let position = self.position();
        self.input
            .read_exact(&mut bytes)
            .map_err(|error| positioned_error(error, position))?;
        Ok(Some(decode_modified_utf8(&bytes)?))
    }
    pub fn bytes(&mut self, max: usize) -> io::Result<Option<Vec<u8>>> {
        if !self.bool()? {
            return Ok(None);
        }
        let length =
            usize::try_from(self.i32()?).map_err(|_| invalid("negative byte sequence length"))?;
        if length > max {
            return Err(invalid("OpenWire byte sequence exceeds configured maximum"));
        }
        let mut bytes = vec![0; length];
        let position = self.position();
        self.input
            .read_exact(&mut bytes)
            .map_err(|error| positioned_error(error, position))?;
        Ok(Some(bytes))
    }
    pub fn header(&mut self) -> io::Result<CommandHeader> {
        Ok(CommandHeader {
            id: self.i32()?,
            response_required: self.bool()?,
        })
    }
    pub fn nested(&mut self) -> io::Result<Option<(u8, Decoder<'a>)>> {
        self.nested_version(12)
    }

    pub fn nested_version(&mut self, version: i32) -> io::Result<Option<(u8, Decoder<'a>)>> {
        if !self.bool()? {
            return Ok(None);
        }
        let kind = self.u8()?;
        let start = self.position() as usize;
        let end = nested_end_version(self.input.get_ref(), start, kind, version)?;
        let data = &self.input.get_ref()[start..end];
        self.input.set_position(end as u64);
        Ok(Some((kind, Decoder::new(data))))
    }
    pub fn destination(&mut self) -> io::Result<Option<WireDestination>> {
        let Some((kind, mut d)) = self.nested()? else {
            return Ok(None);
        };
        if !matches!(kind, 100..=103) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected ActiveMQ destination, got type {kind}"),
            ));
        }
        Ok(Some(WireDestination {
            kind,
            name: d
                .string()?
                .ok_or_else(|| invalid("null destination name"))?,
        }))
    }
    pub fn id(&mut self, expected: u8) -> io::Result<Option<WireId>> {
        let Some((kind, mut d)) = self.nested()? else {
            return Ok(None);
        };
        if kind != expected {
            return Err(invalid("unexpected OpenWire identifier type"));
        }
        let connection = d
            .string()?
            .ok_or_else(|| invalid("null OpenWire connection id"))?;
        let (session, value) = match kind {
            120 => (0, 0),
            121 => (d.i64()?, 0),
            122 => (d.i64()?, d.i64()?),
            123 => {
                let value = d.i64()?;
                let session = d.i64()?;
                (session, value)
            }
            _ => return Err(invalid("unsupported OpenWire id")),
        };
        Ok(Some(WireId {
            connection,
            session,
            value,
        }))
    }
    pub fn skip_nested(&mut self) -> io::Result<()> {
        let Some((_, _)) = self.nested()? else {
            return Ok(());
        };
        Ok(())
    }
    pub fn skip_object_array(&mut self, max: usize) -> io::Result<()> {
        if !self.bool()? {
            return Ok(());
        }
        let count = usize::from(self.u16()?);
        if count > max {
            return Err(invalid("oversized OpenWire object array"));
        }
        for _ in 0..count {
            self.skip_nested()?;
        }
        Ok(())
    }
}

fn nested_end_version(bytes: &[u8], start: usize, kind: u8, version: i32) -> io::Result<usize> {
    let mut d = Decoder::new(
        bytes
            .get(start..)
            .ok_or_else(|| invalid("invalid nested object"))?,
    );
    match kind {
        100..=103 | 124 => {
            d.string()?;
        }
        110 => {
            if version >= 10 {
                d.string()?;
            }
            d.skip_nested()?;
            d.i64()?;
            d.i64()?;
        }
        120 => {
            d.string()?;
        }
        121 => {
            d.string()?;
            d.i64()?;
        }
        122 | 123 => {
            d.string()?;
            d.i64()?;
            d.i64()?;
        }
        111 => {
            d.i64()?;
            d.skip_nested()?;
        }
        112 => {
            d.i32()?;
            d.bytes(64 * 1024)?;
            d.bytes(64 * 1024)?;
        }
        3 => {
            d.header()?;
            d.id(120)?;
            d.string()?;
            d.string()?;
            d.string()?;
            skip_broker_path(&mut d)?;
            let flags = if version >= 6 {
                5
            } else if version == 1 {
                2
            } else {
                3
            };
            for _ in 0..flags {
                d.bool()?;
            }
            if version >= 8 {
                d.string()?;
            }
        }
        5 => {
            d.header()?;
            d.id(122)?;
            d.bool()?;
            d.destination()?;
            d.i32()?;
            d.i32()?;
            d.bool()?;
            d.string()?;
            if version >= 10 {
                d.string()?;
            }
            d.string()?;
            d.bool()?;
            d.bool()?;
            d.bool()?;
            d.i8()?;
            skip_broker_path(&mut d)?;
            d.skip_nested()?;
            d.bool()?;
            d.bool()?;
            d.bool()?;
            if version >= 4 {
                d.skip_object_array(1024)?;
            }
        }
        6 => {
            d.header()?;
            d.id(123)?;
            d.destination()?;
            skip_broker_path(&mut d)?;
            if version >= 2 {
                d.bool()?;
            }
            if version >= 3 {
                d.i32()?;
            }
        }
        8 => {
            d.header()?;
            d.id(120)?;
            d.destination()?;
            d.i8()?;
            d.i64()?;
            skip_broker_path(&mut d)?;
        }
        12 => {
            d.header()?;
            d.nested()?;
            if version >= 5 {
                d.i64()?;
            }
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported nested OpenWire data structure type {kind}"),
            ));
        }
    }
    Ok(start + d.position() as usize)
}

fn skip_broker_path(d: &mut Decoder<'_>) -> io::Result<()> {
    if d.bool()? {
        let count = usize::from(d.u16()?);
        if count > 1024 {
            return Err(invalid("broker path too large"));
        }
        for _ in 0..count {
            d.skip_nested()?;
        }
    }
    Ok(())
}

#[cfg(test)]
pub fn decode_consumer(frame: &OpenWireFrame) -> io::Result<ConsumerInfo> {
    decode_consumer_version(frame, 12)
}

pub fn decode_consumer_version(frame: &OpenWireFrame, version: i32) -> io::Result<ConsumerInfo> {
    let mut d = Decoder::new(&frame.payload);
    let header = d.header()?;
    let id = d
        .id(122)?
        .ok_or_else(|| invalid("ConsumerInfo is missing ConsumerId"))?;
    let browser = d.bool()?;
    let destination = d.destination()?;
    let prefetch = d.i32()?;
    d.i32()?;
    d.bool()?;
    let selector = d.string()?;
    if version >= 10 {
        d.string()?;
    }
    let subscription_name = d.string()?;
    let no_local = d.bool()?;
    let exclusive = d.bool()?;
    let retroactive = d.bool()?;
    let priority = d.i8()?;
    let mut broker_path = Vec::new();
    if d.bool()? {
        let count = usize::from(d.u16()?);
        if count > 1024 {
            return Err(invalid("broker path too large"));
        }
        for _ in 0..count {
            let (_, mut nested) = d.nested()?.ok_or_else(|| invalid("null broker id"))?;
            broker_path.push(nested.string()?.unwrap_or_default());
        }
    }
    d.skip_nested()?;
    let network = d.bool()?;
    d.bool()?;
    d.bool()?;
    if version >= 4 {
        d.skip_object_array(1024)?;
    }
    Ok(ConsumerInfo {
        header,
        id,
        browser,
        destination: destination.ok_or_else(|| invalid("ConsumerInfo destination missing"))?,
        prefetch,
        selector,
        subscription_name,
        no_local,
        exclusive,
        retroactive,
        priority,
        network,
        broker_path,
    })
}

#[cfg(test)]
pub fn decode_producer(frame: &OpenWireFrame) -> io::Result<ProducerInfo> {
    decode_producer_version(frame, 12)
}

pub fn decode_producer_version(frame: &OpenWireFrame, version: i32) -> io::Result<ProducerInfo> {
    let mut d = Decoder::new(&frame.payload);
    let header = d.header()?;
    let id = d
        .id(123)?
        .ok_or_else(|| invalid("ProducerInfo is missing ProducerId"))?;
    let destination = d.destination()?;
    if d.bool()? {
        let n = usize::from(d.u16()?);
        if n > 1024 {
            return Err(invalid("broker path too large"));
        }
        for _ in 0..n {
            d.skip_nested()?;
        }
    }
    if version >= 2 {
        d.bool()?;
    }
    let window_size = if version >= 3 { d.i32()? } else { 0 };
    Ok(ProducerInfo {
        header,
        id,
        destination,
        window_size,
    })
}

pub fn decode_destination_info(
    frame: &OpenWireFrame,
    _version: i32,
) -> io::Result<(CommandHeader, Option<WireDestination>, u8)> {
    let mut d = Decoder::new(&frame.payload);
    let header = d.header()?;
    d.id(120)?;
    let destination = d.destination()?;
    let operation = d.u8()?;
    d.i64()?;
    d.skip_object_array(1024)?;
    Ok((header, destination, operation))
}

#[cfg(test)]
pub fn decode_message(frame: &OpenWireFrame, max: usize) -> io::Result<IncomingMessage> {
    decode_message_version(frame, max, 12)
}

pub fn decode_message_version(
    frame: &OpenWireFrame,
    max: usize,
    version: i32,
) -> io::Result<IncomingMessage> {
    decode_message_with_consumed(frame, max, version).map(|(message, _)| message)
}

fn decode_message_with_consumed(
    frame: &OpenWireFrame,
    max: usize,
    version: i32,
) -> io::Result<(IncomingMessage, usize)> {
    let mut d = Decoder::new(&frame.payload);
    let header = d.header()?;
    let producer_id = d.id(123)?;
    let destination = d.destination()?;
    let tx = decode_tx(d.nested()?)?;
    let _original = d.destination()?;
    let msg_id = decode_message_id(&mut d, version)?.unwrap_or_default();
    d.skip_nested()?;
    let group_id = d.string()?;
    let group_sequence = d.i32()?;
    let correlation_id = d.string()?;
    let persistent = d.bool()?;
    let expiration = d.i64()?;
    let priority = d.u8()?;
    let reply_to = d.destination()?;
    let timestamp = d.i64()?;
    let message_type = d.string()?;
    let body = d.bytes(max)?.unwrap_or_default();
    let properties = d.bytes(max)?.unwrap_or_default();
    let advisory = decode_advisory(d.nested_version(version)?, version, destination.as_ref())?;
    d.skip_nested()?;
    let compressed = d.bool()?;
    let _redelivery = d.i32()?;
    let mut broker_path = Vec::new();
    if d.bool()? {
        let n = usize::from(d.u16()?);
        if n > 1024 {
            return Err(invalid("broker path too large"));
        }
        for _ in 0..n {
            let (_, mut b) = d.nested()?.ok_or_else(|| invalid("null broker path"))?;
            broker_path.push(b.string()?.unwrap_or_default());
        }
    }
    d.i64()?;
    d.string()?;
    d.bool()?;
    if version >= 2 {
        d.bool()?;
    }
    if version >= 3 {
        d.skip_object_array(1024)?;
        d.i64()?;
        d.i64()?;
    }
    if version >= 10 {
        d.bool()?;
    }
    Ok((
        IncomingMessage {
            header,
            tx,
            producer_id,
            destination,
            message_id: msg_id,
            correlation_id,
            reply_to,
            group_id,
            group_sequence,
            persistent,
            expiration,
            timestamp,
            priority,
            message_type,
            properties,
            body,
            compressed,
            broker_path,
            command_type: frame.command_type,
            advisory,
        },
        d.position() as usize,
    ))
}

fn decode_advisory(
    object: Option<(u8, Decoder<'_>)>,
    version: i32,
    topic: Option<&WireDestination>,
) -> io::Result<Option<crate::core::AdvisoryData>> {
    use crate::core::{AdvisoryData, Destination};
    let Some((kind, mut d)) = object else {
        return Ok(None);
    };
    let data = match kind {
        5 => {
            let _header = d.header()?;
            let id = d
                .id(122)?
                .ok_or_else(|| invalid("advisory ConsumerInfo ID missing"))?;
            d.bool()?;
            let destination = d
                .destination()?
                .ok_or_else(|| invalid("advisory ConsumerInfo destination missing"))?;
            let prefetch = d.i32()?.max(0) as u32;
            d.i32()?;
            d.bool()?;
            let selector = d.string()?;
            let client_id = if version >= 10 { d.string()? } else { None };
            let durable_name = d.string()?;
            d.bool()?;
            d.bool()?;
            d.bool()?;
            d.i8()?;
            let broker_path = decode_broker_path(&mut d)?;
            d.skip_nested()?;
            let network = d.bool()?;
            d.bool()?;
            d.bool()?;
            if version >= 4 {
                d.skip_object_array(1024)?;
            }
            let core_destination =
                Destination::new(destination_kind(&destination), destination.name);
            Some(AdvisoryData::ConsumerAdded {
                connection: numeric_wire_id(&id),
                consumer: id.value.max(0) as u64,
                external_id: Some(id.display()),
                destination: core_destination,
                selector,
                durable_name,
                client_id,
                network,
                broker_path,
                prefetch,
            })
        }
        6 => {
            let _header = d.header()?;
            let id = d
                .id(123)?
                .ok_or_else(|| invalid("advisory ProducerInfo ID missing"))?;
            let destination = d
                .destination()?
                .map(|d| Destination::new(destination_kind(&d), d.name));
            let _ = decode_broker_path(&mut d)?;
            if version >= 2 {
                d.bool()?;
            }
            if version >= 3 {
                d.i32()?;
            }
            Some(AdvisoryData::ProducerAdded {
                connection: numeric_wire_id(&id),
                producer: id.value.max(0) as u64,
                destination,
            })
        }
        8 => {
            let _header = d.header()?;
            d.id(120)?;
            let destination = d
                .destination()?
                .ok_or_else(|| invalid("advisory DestinationInfo destination missing"))?;
            let operation = d.u8()?;
            d.i64()?;
            let _ = decode_broker_path(&mut d)?;
            let destination = Destination::new(destination_kind(&destination), destination.name);
            Some(if operation == 0 {
                AdvisoryData::DestinationAdded(destination)
            } else {
                AdvisoryData::DestinationRemoved(destination)
            })
        }
        3 => {
            let _header = d.header()?;
            let id = d
                .id(120)?
                .ok_or_else(|| invalid("advisory ConnectionInfo ID missing"))?;
            let client_id = d.string()?;
            d.string()?;
            let username = d.string()?;
            let _ = decode_broker_path(&mut d)?;
            for _ in 0..if version >= 6 {
                5
            } else if version == 1 {
                2
            } else {
                3
            } {
                d.bool()?;
            }
            let remote_address = if version >= 8 {
                d.string()?.unwrap_or_default()
            } else {
                String::new()
            };
            Some(AdvisoryData::ConnectionAdded {
                connection: numeric_wire_id(&id),
                client_id,
                username,
                remote_address,
            })
        }
        12 => {
            let _header = d.header()?;
            let Some((id_kind, id)) = d.nested()? else {
                return Ok(None);
            };
            let wire_id = decode_wire_id_body(id_kind, id)?;
            if version >= 5 {
                d.i64()?;
            }
            match id_kind {
                120 => Some(AdvisoryData::ConnectionRemoved {
                    connection: numeric_wire_id(&wire_id),
                    client_id: None,
                }),
                122 => Some(AdvisoryData::ConsumerRemoved {
                    connection: numeric_wire_id(&wire_id),
                    consumer: wire_id.value.max(0) as u64,
                    external_id: Some(wire_id.display()),
                    destination: advisory_destination(topic),
                }),
                123 => Some(AdvisoryData::ProducerRemoved {
                    connection: numeric_wire_id(&wire_id),
                    producer: wire_id.value.max(0) as u64,
                }),
                _ => None,
            }
        }
        _ => None,
    };
    Ok(data)
}

fn advisory_destination(topic: Option<&WireDestination>) -> crate::core::Destination {
    let Some(topic) = topic else {
        return crate::core::Destination::queue("");
    };
    let name = topic.name.as_str();
    for (prefix, kind) in [
        (
            "ActiveMQ.Advisory.Consumer.Queue.",
            crate::core::DestinationKind::Queue,
        ),
        (
            "ActiveMQ.Advisory.Consumer.Topic.",
            crate::core::DestinationKind::Topic,
        ),
        (
            "ActiveMQ.Advisory.Consumer.TempQueue.",
            crate::core::DestinationKind::TempQueue,
        ),
        (
            "ActiveMQ.Advisory.Consumer.TempTopic.",
            crate::core::DestinationKind::TempTopic,
        ),
    ] {
        if let Some(destination) = name.strip_prefix(prefix) {
            return crate::core::Destination::new(kind, destination);
        }
    }
    crate::core::Destination::queue("")
}

fn decode_broker_path(d: &mut Decoder<'_>) -> io::Result<Vec<String>> {
    if !d.bool()? {
        return Ok(Vec::new());
    }
    let count = usize::from(d.u16()?);
    if count > 1024 {
        return Err(invalid("broker path too large"));
    }
    let mut path = Vec::with_capacity(count);
    for _ in 0..count {
        let (_, mut broker) = d.nested()?.ok_or_else(|| invalid("null broker path ID"))?;
        path.push(broker.string()?.unwrap_or_default());
    }
    Ok(path)
}

fn decode_wire_id_body(kind: u8, mut d: Decoder<'_>) -> io::Result<WireId> {
    let connection = d.string()?.unwrap_or_default();
    let (session, value) = match kind {
        120 => (0, 0),
        121 => (d.i64()?, 0),
        122 => (d.i64()?, d.i64()?),
        123 => {
            let value = d.i64()?;
            (d.i64()?, value)
        }
        _ => return Err(invalid("unsupported advisory ID type")),
    };
    Ok(WireId {
        connection,
        session,
        value,
    })
}

fn numeric_wire_id(id: &WireId) -> u64 {
    id.connection
        .rsplit_once('-')
        .and_then(|(_, suffix)| suffix.parse().ok())
        .unwrap_or_else(|| id.value.max(0) as u64)
}

fn decode_message_id(d: &mut Decoder<'_>, version: i32) -> io::Result<Option<String>> {
    let Some((kind, mut obj)) = d.nested_version(version)? else {
        return Ok(None);
    };
    if kind != 110 {
        return Err(invalid("expected MessageId"));
    }
    let text = if version >= 10 { obj.string()? } else { None };
    let producer = obj.id(123)?;
    let sequence = obj.i64()?;
    obj.i64()?;
    Ok(Some(text.unwrap_or_else(|| {
        producer
            .map(|p| format_message_id(&p, sequence))
            .unwrap_or_default()
    })))
}

#[cfg(test)]
pub fn decode_ack(frame: &OpenWireFrame) -> io::Result<AckInfo> {
    decode_ack_version(frame, 12)
}

pub fn decode_ack_version(frame: &OpenWireFrame, version: i32) -> io::Result<AckInfo> {
    let mut d = Decoder::new(&frame.payload);
    let header = d.header()?;
    d.destination()?;
    let tx = decode_tx(d.nested()?)?;
    let consumer = d
        .id(122)?
        .ok_or_else(|| invalid("MessageAck is missing ConsumerId"))?;
    let ack_type = d.u8()?;
    let _first = decode_message_id(&mut d, version)?;
    let message_id = decode_message_id(&mut d, version)?;
    let count = d.i32()?;
    if version >= 7 {
        let _poison_present = d.bool()?;
    }
    Ok(AckInfo {
        header,
        consumer,
        ack_type,
        message_id,
        count,
        tx,
    })
}

fn decode_tx(tx: Option<(u8, Decoder<'_>)>) -> io::Result<Option<crate::core::TxId>> {
    let Some((kind, mut obj)) = tx else {
        return Ok(None);
    };
    Ok(match kind {
        111 => {
            let value = obj.i64()?;
            obj.id(120)?;
            Some(crate::core::TxId::Local(value.to_string()))
        }
        112 => {
            let format_id = obj.i32()?;
            let global_id = obj.bytes(64 * 1024)?.unwrap_or_default();
            let branch_id = obj.bytes(64 * 1024)?.unwrap_or_default();
            Some(crate::core::TxId::Xa(crate::core::store::XidRecord {
                format_id,
                global_id,
                branch_id,
            }))
        }
        _ => None,
    })
}

pub fn decode_transaction(
    frame: &OpenWireFrame,
) -> io::Result<(CommandHeader, u8, Option<crate::core::TxId>)> {
    let mut d = Decoder::new(&frame.payload);
    let header = d.header()?;
    let _connection = d.id(120)?;
    let tx = d.nested()?;
    let tx = decode_tx(tx)?;
    Ok((header, d.u8()?, tx))
}

pub fn integer_response(header: &CommandHeader, value: i32) -> OpenWireFrame {
    let mut p = base(0, false);
    p.extend_from_slice(&header.id.to_be_bytes());
    p.extend_from_slice(&value.to_be_bytes());
    OpenWireFrame {
        command_type: 34,
        payload: p,
    }
}
pub fn xid_array_response(
    header: &CommandHeader,
    xids: &[crate::core::store::XidRecord],
) -> OpenWireFrame {
    let mut p = base(0, false);
    p.extend_from_slice(&header.id.to_be_bytes());
    p.extend_from_slice(&(xids.len() as u16).to_be_bytes());
    for xid in xids {
        p.push(1);
        p.push(112);
        p.extend_from_slice(&xid.format_id.to_be_bytes());
        write_byte_sequence(&mut p, Some(&xid.global_id));
        write_byte_sequence(&mut p, Some(&xid.branch_id));
    }
    OpenWireFrame {
        command_type: 33,
        payload: p,
    }
}

pub fn response(header: &CommandHeader) -> OpenWireFrame {
    let mut p = Vec::new();
    p.extend_from_slice(&0i32.to_be_bytes());
    p.push(0);
    p.extend_from_slice(&header.id.to_be_bytes());
    OpenWireFrame {
        command_type: 30,
        payload: p,
    }
}

pub fn exception(header: &CommandHeader, class: &str, message: &str) -> OpenWireFrame {
    let mut p = Vec::new();
    p.extend_from_slice(&0i32.to_be_bytes());
    p.push(0);
    p.extend_from_slice(&header.id.to_be_bytes());
    p.push(1);
    let _ = write_string(&mut p, Some(class));
    let _ = write_string(&mut p, Some(message));
    OpenWireFrame {
        command_type: 31,
        payload: p,
    }
}

pub fn encode_broker_info(id: &str, name: &str, version: i32) -> OpenWireFrame {
    let mut p = base(0, false);
    nested_string(&mut p, 124, Some(id));
    let _ = write_string(&mut p, Some(""));
    p.push(0);
    let _ = write_string(&mut p, Some(name));
    p.extend_from_slice(&[0; 3]);
    if version >= 2 {
        p.extend_from_slice(&[0; 2]);
        p.extend_from_slice(&0i64.to_be_bytes());
    }
    if version >= 3 {
        let _ = write_string(&mut p, None);
        let _ = write_string(&mut p, None);
    }
    OpenWireFrame {
        command_type: 2,
        payload: p,
    }
}

pub fn decode_broker_info(frame: &OpenWireFrame) -> io::Result<Option<String>> {
    if frame.command_type != 2 {
        return Err(invalid("expected BrokerInfo"));
    }
    let mut d = Decoder::new(&frame.payload);
    d.header()?;
    let Some((kind, mut broker_id)) = d.nested()? else {
        return Ok(None);
    };
    if kind != 124 {
        return Err(invalid("BrokerInfo has an invalid BrokerId"));
    }
    broker_id.string()
}

pub fn encode_wire_format_info(name: &str, frame_limit: usize, version: i32) -> OpenWireFrame {
    let props = [
        ("CacheEnabled", P::Bool(false)),
        ("TightEncodingEnabled", P::Bool(false)),
        ("StackTraceEnabled", P::Bool(false)),
        ("MaxInactivityDuration", P::Long(30000)),
        ("MaxInactivityDurationInitalDelay", P::Long(10000)),
        ("MaxFrameSize", P::Long(frame_limit as i64)),
        ("ProviderName", P::String(name)),
    ];
    let mut map = Vec::new();
    map.extend_from_slice(&(props.len() as i32).to_be_bytes());
    for (k, v) in props {
        let _ = write_java_utf(&mut map, k);
        match v {
            P::Bool(x) => {
                map.push(1);
                map.push(u8::from(x));
            }
            P::Long(x) => {
                map.push(6);
                map.extend_from_slice(&x.to_be_bytes());
            }
            P::String(x) => {
                map.push(9);
                let _ = write_java_utf(&mut map, x);
            }
        }
    }
    let mut p = b"ActiveMQ".to_vec();
    p.extend_from_slice(&version.to_be_bytes());
    p.push(1);
    p.extend_from_slice(&(map.len() as i32).to_be_bytes());
    p.extend(map);
    OpenWireFrame {
        command_type: 1,
        payload: p,
    }
}

enum P<'a> {
    Bool(bool),
    Long(i64),
    String(&'a str),
}

pub fn validate_wire_format(frame: &OpenWireFrame) -> io::Result<(i32, Option<u64>)> {
    if frame.command_type != 1 {
        return Err(invalid("first command is not WireFormatInfo"));
    }
    let mut d = Decoder::new(&frame.payload);
    let mut magic = [0; 8];
    d.input.read_exact(&mut magic)?;
    if &magic != b"ActiveMQ" {
        return Err(invalid("invalid OpenWire magic"));
    }
    let version = d.i32()?;
    if !(1..=12).contains(&version) {
        return Err(invalid("unsupported OpenWire version"));
    }
    let properties = d.bytes(frame.payload.len())?;
    if d.position() as usize != frame.payload.len() {
        return Err(invalid("unexpected bytes after WireFormatInfo"));
    }
    let inactivity = properties
        .as_deref()
        .map(parse_inactivity)
        .transpose()?
        .flatten();
    Ok((version, inactivity))
}
fn parse_inactivity(bytes: &[u8]) -> io::Result<Option<u64>> {
    let mut d = Decoder::new(bytes);
    let count =
        usize::try_from(d.i32()?).map_err(|_| invalid("negative WireFormatInfo property count"))?;
    if count > 256 {
        return Err(invalid("too many WireFormatInfo properties"));
    };
    let mut timeout = None;
    for _ in 0..count {
        let key = read_java_utf(&mut d.input)?;
        let kind = d.u8()?;
        match kind {
            1 => {
                let _ = d.bool()?;
            }
            2 => {
                let _ = d.u8()?;
            }
            3 | 4 => {
                let _ = d.u16()?;
            }
            5 => {
                let _ = d.i32()?;
            }
            6 => {
                let value = d.i64()?;
                if key == "MaxInactivityDuration" && value > 0 {
                    timeout = Some(value as u64)
                }
            }
            7 => {
                let _ = d.i64()?;
            }
            8 => {
                let _ = d.i32()?;
            }
            9 => {
                let _ = read_java_utf(&mut d.input)?;
            }
            _ => return Err(invalid("unsupported WireFormatInfo property type")),
        }
    }
    Ok(timeout)
}

pub type ConnectionInfoFields = (
    CommandHeader,
    Option<String>,
    Option<String>,
    Option<String>,
);

pub fn decode_connection_info_version(
    frame: &OpenWireFrame,
    version: i32,
) -> io::Result<ConnectionInfoFields> {
    let mut d = Decoder::new(&frame.payload);
    let h = d.header()?;
    let _id = d.id(120)?;
    let client = d.string()?;
    let password = d.string()?;
    let user = d.string()?;
    if d.bool()? {
        let n = usize::from(d.u16()?);
        if n > 1024 {
            return Err(invalid("broker path too large"));
        }
        for _ in 0..n {
            d.skip_nested()?;
        }
    }
    let flags = if version >= 6 {
        5
    } else if version == 1 {
        2
    } else {
        3
    };
    for _ in 0..flags {
        d.bool()?;
    }
    if version >= 8 {
        d.string()?;
    }
    Ok((h, client, user, password))
}

pub fn decode_remove_version(
    frame: &OpenWireFrame,
    version: i32,
) -> io::Result<(CommandHeader, Option<(u8, WireId)>)> {
    let mut d = Decoder::new(&frame.payload);
    let h = d.header()?;
    let Some((kind, mut obj)) = d.nested()? else {
        return Ok((h, None));
    };
    let connection = obj.string()?.unwrap_or_default();
    let id = match kind {
        120 => WireId {
            connection,
            session: 0,
            value: 0,
        },
        121 => WireId {
            connection,
            session: obj.i64()?,
            value: 0,
        },
        122 => WireId {
            connection,
            session: obj.i64()?,
            value: obj.i64()?,
        },
        123 => {
            let value = obj.i64()?;
            let session = obj.i64()?;
            WireId {
                connection,
                session,
                value,
            }
        }
        _ => return Err(invalid("unsupported RemoveInfo identifier")),
    };
    if version >= 5 {
        d.i64()?;
    }
    Ok((h, Some((kind, id))))
}

pub fn decode_pull(frame: &OpenWireFrame) -> io::Result<(CommandHeader, WireId, i64)> {
    let mut d = Decoder::new(&frame.payload);
    let h = d.header()?;
    let id = d
        .id(122)?
        .ok_or_else(|| invalid("MessagePull ConsumerId missing"))?;
    d.destination()?;
    let timeout = d.i64()?;
    let _correlation = d.string()?;
    d.skip_nested()?;
    Ok((h, id, timeout))
}

pub fn encode_dispatch_version(
    consumer: &WireId,
    destination: &WireDestination,
    message: &crate::core::Message,
    _command_id: i64,
    redelivery: u32,
    version: i32,
) -> OpenWireFrame {
    let (producer, sequence) = dispatch_identity(&message.message_id);
    let mut p = base(0, false);
    nested_id(&mut p, 122, consumer);
    nested_destination(&mut p, destination);
    p.push(1);
    p.push(message_type_code(&message.body));
    p.extend_from_slice(&base(0, false));
    nested_id(&mut p, 123, &producer);
    nested_destination(&mut p, destination);
    p.push(0);
    p.push(0);
    p.push(1);
    p.push(110);
    if version >= 10 {
        let _ = write_string(&mut p, Some(&message.message_id));
    }
    nested_id(&mut p, 123, &producer);
    p.extend_from_slice(&sequence.to_be_bytes());
    p.extend_from_slice(&0i64.to_be_bytes());
    p.push(0);
    let _ = write_string(&mut p, message.group_id.as_deref());
    p.extend_from_slice(&message.group_sequence.to_be_bytes());
    let _ = write_string(&mut p, message.correlation_id.as_deref());
    p.push(u8::from(message.persistent));
    p.extend_from_slice(&message.expiration.to_be_bytes());
    p.push(message.priority);
    nested_optional_destination(&mut p, message.reply_to.as_ref());
    p.extend_from_slice(&message.timestamp.to_be_bytes());
    let _ = write_string(&mut p, message.message_type.as_deref());
    write_byte_sequence(&mut p, Some(&body_bytes(&message.body)));
    if message.properties.is_empty() {
        write_byte_sequence(&mut p, None);
    } else {
        write_byte_sequence(&mut p, Some(&marshal_properties(&message.properties)));
    }
    if let Some(advisory) = &message.advisory {
        encode_advisory(&mut p, advisory, version);
    } else {
        p.push(0);
    }
    p.push(0);
    p.push(0);
    p.extend_from_slice(&(redelivery as i32).to_be_bytes());
    if message.broker_path.is_empty() {
        p.push(0);
    } else {
        p.push(1);
        p.extend_from_slice(&(message.broker_path.len() as u16).to_be_bytes());
        for broker in &message.broker_path {
            nested_string(&mut p, 124, Some(broker));
        }
    }
    p.extend_from_slice(&0i64.to_be_bytes());
    let _ = write_string(&mut p, message.user_id.as_deref());
    p.push(0);
    if version >= 2 {
        p.push(0);
    }
    if version >= 3 {
        p.push(0);
        p.extend_from_slice(&0i64.to_be_bytes());
        p.extend_from_slice(&message.broker_in_time.to_be_bytes());
        if version >= 10 {
            p.push(0);
        }
    }
    p.extend_from_slice(&(redelivery as i32).to_be_bytes());
    OpenWireFrame {
        command_type: 21,
        payload: p,
    }
}

fn format_message_id(producer: &WireId, sequence: i64) -> String {
    format!(
        "{}:{}:{}:{sequence}",
        producer.connection, producer.session, producer.value
    )
}

fn dispatch_identity(message_id: &str) -> (WireId, i64) {
    if let Some((producer, sequence)) = message_id.rsplit_once(':')
        && let Some(producer) = parse_wire_id(producer)
        && let Ok(sequence) = sequence.parse()
    {
        return (producer, sequence);
    }
    // Legacy MessageId has no textView. Preserve opaque native IDs in the
    // producer namespace so redelivery/reconnect keeps a stable wire identity.
    (
        WireId {
            connection: message_id.to_owned(),
            session: 1,
            value: 0,
        },
        0,
    )
}

pub fn dispatched_message_id(message_id: &str, version: i32) -> String {
    if version >= 10 {
        return message_id.to_owned();
    }
    let (producer, sequence) = dispatch_identity(message_id);
    format_message_id(&producer, sequence)
}

fn encode_advisory(out: &mut Vec<u8>, advisory: &crate::core::AdvisoryData, version: i32) {
    use crate::core::AdvisoryData;
    match advisory {
        AdvisoryData::DestinationAdded(destination)
        | AdvisoryData::DestinationRemoved(destination) => {
            out.push(1);
            out.push(8);
            out.extend_from_slice(&base(0, false));
            out.push(0);
            nested_destination(out, &wire_destination(destination));
            out.push(u8::from(matches!(
                advisory,
                AdvisoryData::DestinationRemoved(_)
            )));
            out.extend_from_slice(&0i64.to_be_bytes());
            out.push(0);
        }
        AdvisoryData::ConsumerAdded {
            consumer,
            external_id,
            destination,
            selector,
            durable_name,
            client_id,
            network,
            broker_path,
            prefetch,
            ..
        } => {
            let id = external_id
                .as_deref()
                .and_then(parse_wire_id)
                .unwrap_or(WireId {
                    connection: format!("rust-{consumer}"),
                    session: 1,
                    value: *consumer as i64,
                });
            out.push(1);
            out.push(5);
            out.extend_from_slice(&base(0, false));
            nested_id(out, 122, &id);
            out.push(0);
            nested_destination(out, &wire_destination(destination));
            out.extend_from_slice(&(*prefetch as i32).to_be_bytes());
            out.extend_from_slice(&0i32.to_be_bytes());
            out.push(1);
            let _ = write_string(out, selector.as_deref());
            if version >= 10 {
                let _ = write_string(out, client_id.as_deref());
            }
            let _ = write_string(out, durable_name.as_deref());
            out.push(0);
            out.push(0);
            out.push(0);
            out.push(0);
            if broker_path.is_empty() {
                out.push(0)
            } else {
                out.push(1);
                out.extend_from_slice(&(broker_path.len() as u16).to_be_bytes());
                for broker in broker_path {
                    nested_string(out, 124, Some(broker));
                }
            }
            out.push(0);
            out.push(u8::from(*network));
            out.push(0);
            out.push(0);
            if version >= 4 {
                out.push(0);
            }
        }
        AdvisoryData::ConsumerRemoved {
            consumer,
            external_id,
            destination,
            ..
        } => {
            let id = external_id
                .as_deref()
                .and_then(parse_wire_id)
                .unwrap_or(WireId {
                    connection: format!("rust-{consumer}"),
                    session: 1,
                    value: *consumer as i64,
                });
            out.push(1);
            out.push(12);
            out.extend_from_slice(&base(0, false));
            nested_id(out, 122, &id);
            let _ = destination;
            if version >= 5 {
                out.extend_from_slice(&0i64.to_be_bytes());
            }
        }
        AdvisoryData::ProducerAdded {
            connection,
            producer,
            destination,
        } => {
            out.push(1);
            out.push(6);
            out.extend_from_slice(&base(0, false));
            nested_id(
                out,
                123,
                &WireId {
                    connection: format!("rust-{connection}"),
                    session: 1,
                    value: *producer as i64,
                },
            );
            if let Some(destination) = destination {
                nested_destination(out, &wire_destination(destination));
            } else {
                out.push(0);
            }
            out.push(0);
            if version >= 2 {
                out.push(0);
            }
            if version >= 3 {
                out.extend_from_slice(&0i32.to_be_bytes());
            }
        }
        AdvisoryData::ProducerRemoved {
            connection,
            producer,
        } => {
            out.push(1);
            out.push(12);
            out.extend_from_slice(&base(0, false));
            nested_id(
                out,
                123,
                &WireId {
                    connection: format!("rust-{connection}"),
                    session: 1,
                    value: *producer as i64,
                },
            );
            if version >= 5 {
                out.extend_from_slice(&0i64.to_be_bytes());
            }
        }
        AdvisoryData::ConnectionAdded {
            connection,
            client_id,
            username,
            remote_address,
        } => {
            out.push(1);
            out.push(3);
            out.extend_from_slice(&base(0, false));
            nested_id(
                out,
                120,
                &WireId {
                    connection: format!("rust-{connection}"),
                    session: 0,
                    value: 0,
                },
            );
            let _ = write_string(out, client_id.as_deref());
            let _ = write_string(out, None);
            let _ = write_string(out, username.as_deref());
            out.push(0);
            let flags = if version >= 6 {
                5
            } else if version == 1 {
                2
            } else {
                3
            };
            out.extend(std::iter::repeat_n(0, flags));
            if version >= 8 {
                let _ = write_string(out, Some(remote_address));
            }
        }
        AdvisoryData::ConnectionRemoved { connection, .. } => {
            out.push(1);
            out.push(12);
            out.extend_from_slice(&base(0, false));
            nested_id(
                out,
                120,
                &WireId {
                    connection: format!("rust-{connection}"),
                    session: 0,
                    value: 0,
                },
            );
            if version >= 5 {
                out.extend_from_slice(&0i64.to_be_bytes());
            }
        }
    }
}
fn parse_wire_id(text: &str) -> Option<WireId> {
    let mut parts = text.rsplitn(3, ':');
    let value = parts.next()?.parse().ok()?;
    let session = parts.next()?.parse().ok()?;
    let connection = parts.next()?.to_owned();
    Some(WireId {
        connection,
        session,
        value,
    })
}

fn body_bytes(body: &crate::core::Body) -> Vec<u8> {
    match body {
        crate::core::Body::Text(s) => {
            let mut b = Vec::new();
            let _ = write_primitive_string(&mut b, Some(s));
            b
        }
        crate::core::Body::Bytes(b) | crate::core::Body::Object(b) => b.clone(),
        crate::core::Body::Empty => Vec::new(),
        crate::core::Body::Map(m) => marshal_properties(m),
        crate::core::Body::Stream(v) => marshal_stream(v),
    }
}
fn message_type_code(body: &crate::core::Body) -> u8 {
    match body {
        crate::core::Body::Text(_) => 28,
        crate::core::Body::Bytes(_) => 24,
        crate::core::Body::Map(_) => 25,
        crate::core::Body::Stream(_) => 27,
        crate::core::Body::Object(_) => 26,
        crate::core::Body::Empty => 23,
    }
}
fn marshal_stream(values: &[crate::core::Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        marshal_value(&mut bytes, value);
    }
    bytes
}

pub fn marshal_properties(
    properties: &std::collections::BTreeMap<String, crate::core::Value>,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(properties.len() as i32).to_be_bytes());
    for (k, v) in properties {
        let _ = write_java_utf(&mut out, k);
        marshal_value(&mut out, v);
    }
    out
}
fn marshal_value(out: &mut Vec<u8>, v: &crate::core::Value) {
    use crate::core::Value::*;
    match v {
        Null => out.push(0),
        Bool(x) => {
            out.push(1);
            out.push(u8::from(*x))
        }
        Byte(x) => {
            out.push(2);
            out.push(*x as u8)
        }
        Char(x) => {
            out.push(3);
            out.extend_from_slice(&x.to_be_bytes())
        }
        Short(x) => {
            out.push(4);
            out.extend_from_slice(&x.to_be_bytes())
        }
        Int(x) => {
            out.push(5);
            out.extend_from_slice(&x.to_be_bytes())
        }
        Long(x) => {
            out.push(6);
            out.extend_from_slice(&x.to_be_bytes())
        }
        Double(x) => {
            out.push(7);
            out.extend_from_slice(&x.to_bits().to_be_bytes())
        }
        Float(x) => {
            out.push(8);
            out.extend_from_slice(&x.to_bits().to_be_bytes())
        }
        String(x) => {
            out.push(9);
            let _ = write_java_utf(out, x);
        }
        Bytes(x) => {
            out.push(10);
            out.extend_from_slice(&(x.len() as i32).to_be_bytes());
            out.extend_from_slice(x)
        }
        Map(x) => {
            out.push(11);
            out.extend(marshal_properties(x))
        }
        List(x) => {
            out.push(12);
            out.extend_from_slice(&(x.len() as i32).to_be_bytes());
            for item in x {
                marshal_value(out, item)
            }
        }
    }
}

pub fn unmarshal_properties(
    bytes: &[u8],
) -> io::Result<std::collections::BTreeMap<String, crate::core::Value>> {
    if bytes.is_empty() {
        return Ok(std::collections::BTreeMap::new());
    }
    let mut d = Decoder::new(bytes);
    let count = d.i32()?;
    if count < 0 {
        return Ok(std::collections::BTreeMap::new());
    };
    let n = count as usize;
    if n > 4096 {
        return Err(invalid("too many message properties"));
    };
    let mut map = std::collections::BTreeMap::new();
    for _ in 0..n {
        let key = read_java_utf(&mut d.input)?;
        map.insert(key, unmarshal_value(&mut d, 0)?);
    }
    Ok(map)
}
fn unmarshal_value(d: &mut Decoder<'_>, depth: u8) -> io::Result<crate::core::Value> {
    use crate::core::Value;
    if depth > 16 {
        return Err(invalid("OpenWire primitive nesting is too deep"));
    };
    Ok(match d.u8()? {
        0 => Value::Null,
        1 => Value::Bool(d.bool()?),
        2 => Value::Byte(d.i8()?),
        3 => Value::Char(d.u16()?),
        4 => Value::Short(d.i16()?),
        5 => Value::Int(d.i32()?),
        6 => Value::Long(d.i64()?),
        7 => Value::Double(f64::from_bits(u64::from_be_bytes(read_array(
            &mut d.input,
        )?))),
        8 => Value::Float(f32::from_bits(u32::from_be_bytes(read_array(
            &mut d.input,
        )?))),
        9 => Value::String(read_java_utf(&mut d.input)?),
        10 => {
            let n = usize::try_from(d.i32()?).map_err(|_| invalid("bad byte property"))?;
            if n > 1024 * 1024 {
                return Err(invalid("oversized byte property"));
            };
            let mut b = vec![0; n];
            d.input.read_exact(&mut b)?;
            Value::Bytes(b)
        }
        11 => {
            let count = d.i32()?;
            if count < 0 {
                return Ok(Value::Null);
            }
            let mut map = std::collections::BTreeMap::new();
            for _ in 0..count {
                map.insert(read_java_utf(&mut d.input)?, unmarshal_value(d, depth + 1)?);
            }
            Value::Map(map)
        }
        12 => {
            let count = usize::try_from(d.i32()?).map_err(|_| invalid("negative list size"))?;
            if count > 4096 {
                return Err(invalid("list too large"));
            };
            let mut list = Vec::with_capacity(count);
            for _ in 0..count {
                list.push(unmarshal_value(d, depth + 1)?);
            }
            Value::List(list)
        }
        13 => {
            let n = usize::try_from(d.i32()?).map_err(|_| invalid("negative big string size"))?;
            if n > 1024 * 1024 {
                return Err(invalid("big string too large"));
            };
            let mut b = vec![0; n];
            d.input.read_exact(&mut b)?;
            Value::String(String::from_utf8_lossy(&b).into_owned())
        }
        kind => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported primitive map value type {kind}"),
            ));
        }
    })
}
pub fn decode_stream(bytes: &[u8]) -> io::Result<Vec<crate::core::Value>> {
    let mut d = Decoder::new(bytes);
    let mut values = Vec::new();
    while d.position() < (bytes.len() as u64) {
        if values.len() >= 65536 {
            return Err(invalid("too many stream values"));
        };
        values.push(unmarshal_value(&mut d, 0)?);
    }
    Ok(values)
}

pub fn decode_text_body(bytes: &[u8]) -> io::Result<String> {
    let mut d = Decoder::new(bytes);
    let n = usize::try_from(d.i32()?).map_err(|_| invalid("invalid text body length"))?;
    if n > bytes.len() {
        return Err(invalid("truncated text body"));
    };
    let mut content = vec![0; n];
    d.input.read_exact(&mut content)?;
    decode_modified_utf8(&content)
}
fn write_primitive_string(out: &mut Vec<u8>, value: Option<&str>) -> io::Result<()> {
    match value {
        None => out.extend_from_slice(&(-1i32).to_be_bytes()),
        Some(s) => {
            let b = encode_modified_utf8(s);
            out.extend_from_slice(&(b.len() as i32).to_be_bytes());
            out.extend(b);
        }
    }
    Ok(())
}

pub fn destination_kind(destination: &WireDestination) -> crate::core::DestinationKind {
    match destination.kind {
        100 => crate::core::DestinationKind::Queue,
        101 => crate::core::DestinationKind::Topic,
        102 => crate::core::DestinationKind::TempQueue,
        103 => crate::core::DestinationKind::TempTopic,
        _ => crate::core::DestinationKind::Queue,
    }
}
pub fn wire_destination(destination: &crate::core::Destination) -> WireDestination {
    let kind = match destination.kind {
        crate::core::DestinationKind::Queue => 100,
        crate::core::DestinationKind::Topic => 101,
        crate::core::DestinationKind::TempQueue => 102,
        crate::core::DestinationKind::TempTopic => 103,
    };
    WireDestination {
        kind,
        name: destination.name.clone(),
    }
}

pub fn nested_destination(out: &mut Vec<u8>, dest: &WireDestination) {
    nested_string(out, dest.kind, Some(&dest.name))
}
fn nested_optional_destination(out: &mut Vec<u8>, dest: Option<&crate::core::Destination>) {
    if let Some(d) = dest {
        nested_destination(out, &wire_destination(d))
    } else {
        out.push(0)
    }
}
pub fn nested_id(out: &mut Vec<u8>, kind: u8, id: &WireId) {
    out.push(1);
    out.push(kind);
    let _ = write_string(out, Some(&id.connection));
    match kind {
        121 => out.extend_from_slice(&id.session.to_be_bytes()),
        122 => {
            out.extend_from_slice(&id.session.to_be_bytes());
            out.extend_from_slice(&id.value.to_be_bytes())
        }
        123 => {
            out.extend_from_slice(&id.value.to_be_bytes());
            out.extend_from_slice(&id.session.to_be_bytes())
        }
        _ => {}
    }
}
fn nested_string(out: &mut Vec<u8>, kind: u8, value: Option<&str>) {
    out.push(1);
    out.push(kind);
    let _ = write_string(out, value);
}

pub fn base(id: i32, response: bool) -> Vec<u8> {
    let mut p = id.to_be_bytes().to_vec();
    p.push(u8::from(response));
    p
}
pub fn write_string(out: &mut Vec<u8>, value: Option<&str>) -> io::Result<()> {
    match value {
        None => out.push(0),
        Some(s) => {
            out.push(1);
            write_java_utf(out, s)?
        }
    }
    Ok(())
}
fn write_byte_sequence(out: &mut Vec<u8>, bytes: Option<&[u8]>) {
    if let Some(b) = bytes {
        out.push(1);
        out.extend_from_slice(&(b.len() as i32).to_be_bytes());
        out.extend_from_slice(b)
    } else {
        out.push(0)
    }
}
fn write_java_utf(out: &mut Vec<u8>, s: &str) -> io::Result<()> {
    let bytes = encode_modified_utf8(s);
    let n = u16::try_from(bytes.len()).map_err(|_| invalid("modified UTF-8 string too long"))?;
    out.extend_from_slice(&n.to_be_bytes());
    out.extend(bytes);
    Ok(())
}
fn encode_modified_utf8(s: &str) -> Vec<u8> {
    let mut b = Vec::new();
    for c in s.encode_utf16() {
        match c {
            1..=0x7f => b.push(c as u8),
            0..=0x7ff => {
                b.push((0xc0 | ((c >> 6) & 0x1f)) as u8);
                b.push((0x80 | (c & 0x3f)) as u8)
            }
            _ => {
                b.push((0xe0 | ((c >> 12) & 0x0f)) as u8);
                b.push((0x80 | ((c >> 6) & 0x3f)) as u8);
                b.push((0x80 | (c & 0x3f)) as u8)
            }
        }
    }
    b
}
fn decode_modified_utf8(bytes: &[u8]) -> io::Result<String> {
    let mut units = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let a = bytes[i];
        if a & 0x80 == 0 {
            units.push(a as u16);
            i += 1
        } else if a & 0xe0 == 0xc0 {
            if i + 1 >= bytes.len() {
                return Err(invalid("invalid modified UTF-8"));
            };
            units.push((((a & 0x1f) as u16) << 6) | ((bytes[i + 1] & 0x3f) as u16));
            i += 2
        } else if a & 0xf0 == 0xe0 {
            if i + 2 >= bytes.len() {
                return Err(invalid("invalid modified UTF-8"));
            };
            units.push(
                (((a & 0x0f) as u16) << 12)
                    | (((bytes[i + 1] & 0x3f) as u16) << 6)
                    | ((bytes[i + 2] & 0x3f) as u16),
            );
            i += 3
        } else {
            return Err(invalid("invalid modified UTF-8"));
        }
    }
    String::from_utf16(&units).map_err(|_| invalid("invalid UTF-16 sequence"))
}
fn read_java_utf<R: Read>(r: &mut R) -> io::Result<String> {
    let n = usize::from(u16::from_be_bytes(read_array(r)?));
    let mut b = vec![0; n];
    r.read_exact(&mut b)?;
    decode_modified_utf8(&b)
}
fn read_array<const N: usize>(r: &mut impl Read) -> io::Result<[u8; N]> {
    let mut b = [0; N];
    r.read_exact(&mut b)?;
    Ok(b)
}
fn read_u8(r: &mut impl Read) -> io::Result<u8> {
    Ok(read_array::<1>(r)?[0])
}
fn read_u32(r: &mut impl Read) -> io::Result<u32> {
    Ok(u32::from_be_bytes(read_array(r)?))
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn positioned_error(error: io::Error, position: u64) -> io::Error {
    io::Error::new(error.kind(), format!("{} at byte offset {position}", error))
}

pub fn client_connection_info(
    command_id: i32,
    id: &WireId,
    client_id: Option<&str>,
    username: Option<&str>,
    password: Option<&str>,
) -> OpenWireFrame {
    let mut p = base(command_id, true);
    nested_id(&mut p, 120, id);
    let _ = write_string(&mut p, client_id);
    let _ = write_string(&mut p, password);
    let _ = write_string(&mut p, username);
    p.push(0);
    p.extend_from_slice(&[0; 5]);
    let _ = write_string(&mut p, None);
    OpenWireFrame {
        command_type: 3,
        payload: p,
    }
}
pub fn client_session_info(command_id: i32, id: &WireId) -> OpenWireFrame {
    let mut p = base(command_id, true);
    nested_id(&mut p, 121, id);
    OpenWireFrame {
        command_type: 4,
        payload: p,
    }
}
pub fn client_destination_info(
    command_id: i32,
    connection: &WireId,
    destination: &WireDestination,
    operation: u8,
) -> OpenWireFrame {
    let mut p = base(command_id, true);
    nested_id(&mut p, 120, connection);
    nested_destination(&mut p, destination);
    p.push(operation);
    p.extend_from_slice(&0i64.to_be_bytes());
    p.push(0);
    OpenWireFrame {
        command_type: 8,
        payload: p,
    }
}
pub fn client_remove_info(command_id: i32, id: &WireId) -> OpenWireFrame {
    let mut p = base(command_id, true);
    nested_id(&mut p, 122, id);
    p.extend_from_slice(&0i64.to_be_bytes());
    OpenWireFrame {
        command_type: 12,
        payload: p,
    }
}
pub fn producer_ack(producer: &WireId, size: i32) -> OpenWireFrame {
    let mut p = base(0, false);
    nested_id(&mut p, 123, producer);
    p.extend_from_slice(&size.to_be_bytes());
    OpenWireFrame {
        command_type: 19,
        payload: p,
    }
}
pub fn client_producer_info(
    command_id: i32,
    id: &WireId,
    destination: Option<&WireDestination>,
) -> OpenWireFrame {
    let mut p = base(command_id, true);
    nested_id(&mut p, 123, id);
    if let Some(d) = destination {
        nested_destination(&mut p, d)
    } else {
        p.push(0)
    }
    p.push(0);
    p.push(1);
    p.extend_from_slice(&0i32.to_be_bytes());
    OpenWireFrame {
        command_type: 6,
        payload: p,
    }
}
#[cfg(test)]
pub fn client_consumer_info(
    command_id: i32,
    id: &WireId,
    destination: &WireDestination,
    prefetch: u32,
    selector: Option<&str>,
) -> OpenWireFrame {
    client_consumer_info_options(command_id, id, destination, prefetch, selector, false, &[])
}

pub fn client_consumer_info_options(
    command_id: i32,
    id: &WireId,
    destination: &WireDestination,
    prefetch: u32,
    selector: Option<&str>,
    network: bool,
    broker_path: &[String],
) -> OpenWireFrame {
    let mut p = base(command_id, true);
    nested_id(&mut p, 122, id);
    p.push(0);
    nested_destination(&mut p, destination);
    p.extend_from_slice(&(prefetch as i32).to_be_bytes());
    p.extend_from_slice(&0i32.to_be_bytes());
    p.push(1);
    let _ = write_string(&mut p, selector);
    let _ = write_string(&mut p, None);
    let _ = write_string(&mut p, None);
    p.extend_from_slice(&[0, 0, 0, 0]);
    if broker_path.is_empty() {
        p.push(0);
    } else {
        p.push(1);
        p.extend_from_slice(&(broker_path.len() as u16).to_be_bytes());
        for broker in broker_path {
            nested_string(&mut p, 124, Some(broker));
        }
    }
    p.push(0);
    p.extend_from_slice(&[u8::from(network), 0, 0]);
    p.push(0);
    OpenWireFrame {
        command_type: 5,
        payload: p,
    }
}
pub fn client_message(
    command_id: i32,
    id: &WireId,
    message: &crate::core::Message,
) -> OpenWireFrame {
    let destination = wire_destination(&message.destination);
    let mut p = base(command_id, false);
    nested_id(&mut p, 123, id);
    nested_destination(&mut p, &destination);
    p.push(0);
    p.push(0);
    p.push(1);
    p.push(110);
    let _ = write_string(&mut p, Some(&message.message_id));
    nested_id(&mut p, 123, id);
    p.extend_from_slice(&0i64.to_be_bytes());
    p.extend_from_slice(&0i64.to_be_bytes());
    p.push(0);
    let _ = write_string(&mut p, message.group_id.as_deref());
    p.extend_from_slice(&message.group_sequence.to_be_bytes());
    let _ = write_string(&mut p, message.correlation_id.as_deref());
    p.push(u8::from(message.persistent));
    p.extend_from_slice(&message.expiration.to_be_bytes());
    p.push(message.priority);
    nested_optional_destination(&mut p, message.reply_to.as_ref());
    p.extend_from_slice(&message.timestamp.to_be_bytes());
    let _ = write_string(&mut p, message.message_type.as_deref());
    let content = body_bytes(&message.body);
    write_byte_sequence(&mut p, Some(&content));
    if message.properties.is_empty() {
        write_byte_sequence(&mut p, None)
    } else {
        write_byte_sequence(&mut p, Some(&marshal_properties(&message.properties)))
    }

    if let Some(advisory) = &message.advisory {
        encode_advisory(&mut p, advisory, 12);
    } else {
        p.push(0);
    }
    p.push(0);
    p.push(0);
    p.extend_from_slice(&0i32.to_be_bytes());
    if message.broker_path.is_empty() {
        p.push(0)
    } else {
        p.push(1);
        p.extend_from_slice(&(message.broker_path.len() as u16).to_be_bytes());
        for broker in &message.broker_path {
            nested_string(&mut p, 124, Some(broker));
        }
    }
    p.extend_from_slice(&0i64.to_be_bytes());
    let _ = write_string(&mut p, message.user_id.as_deref());
    p.extend_from_slice(&[0, 0, 0]);
    p.extend_from_slice(&0i64.to_be_bytes());
    p.extend_from_slice(&0i64.to_be_bytes());
    p.push(0);
    OpenWireFrame {
        command_type: message_type_code(&message.body),
        payload: p,
    }
}
pub fn encode_client_message(
    command_id: i32,
    producer: &WireId,
    message: &crate::core::Message,
) -> OpenWireFrame {
    client_message(command_id, producer, message)
}
pub fn client_ack(
    command_id: i32,
    consumer: &WireId,
    destination: &WireDestination,
    message_id: &str,
) -> OpenWireFrame {
    let mut p = base(command_id, false);
    nested_destination(&mut p, destination);
    p.push(0);
    nested_id(&mut p, 122, consumer);
    p.push(2);
    p.push(0);
    p.push(1);
    p.push(110);
    let _ = write_string(&mut p, Some(message_id));
    nested_id(
        &mut p,
        123,
        &WireId {
            connection: "client".into(),
            session: 0,
            value: 0,
        },
    );
    p.extend_from_slice(&0i64.to_be_bytes());
    p.extend_from_slice(&0i64.to_be_bytes());
    p.extend_from_slice(&1i32.to_be_bytes());
    p.push(0);
    OpenWireFrame {
        command_type: 22,
        payload: p,
    }
}
pub fn decode_dispatch(
    frame: &OpenWireFrame,
    max: usize,
) -> io::Result<(WireId, WireDestination, IncomingMessage, u32)> {
    decode_dispatch_version(frame, max, 12)
}

pub fn decode_dispatch_version(
    frame: &OpenWireFrame,
    max: usize,
    version: i32,
) -> io::Result<(WireId, WireDestination, IncomingMessage, u32)> {
    if frame.command_type != 21 {
        return Err(invalid("expected MessageDispatch"));
    };
    let mut d = Decoder::new(&frame.payload);
    let _header = d.header()?;
    let consumer = d
        .id(122)?
        .ok_or_else(|| invalid("dispatch ConsumerId missing"))?;
    let destination = d
        .destination()?
        .ok_or_else(|| invalid("dispatch destination missing"))?;
    if !d.bool()? {
        return Err(invalid("dispatch message missing"));
    }
    let kind = d.u8()?;
    let start = d.position() as usize;
    let payload = frame.payload[start..].to_vec();
    let (incoming, consumed) = decode_message_with_consumed(
        &OpenWireFrame {
            command_type: kind,
            payload,
        },
        max,
        version,
    )?;
    d.input.set_position((start + consumed) as u64);
    let redelivery = d.i32()? as u32;
    Ok((consumer, destination, incoming, redelivery))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frame_round_trip() {
        let f = OpenWireFrame {
            command_type: 10,
            payload: vec![0; 5],
        };
        let mut b = Vec::new();
        f.write_to(&mut b, 1024).unwrap();
        assert_eq!(
            OpenWireFrame::read_from(&mut Cursor::new(b), 1024).unwrap(),
            f
        );
    }
    #[test]
    fn wire_utf_modified_nul_and_supplementary() {
        let s = "hi\0 🧭";
        let mut b = Vec::new();
        write_java_utf(&mut b, s).unwrap();
        assert_eq!(read_java_utf(&mut Cursor::new(b)).unwrap(), s);
    }
    #[test]
    fn rejects_bad_frame_size() {
        assert!(OpenWireFrame::read_from(&mut Cursor::new(0u32.to_be_bytes()), 10).is_err());
    }

    #[test]
    fn consumer_advisory_layouts_include_destination_and_predicate_in_every_version() {
        use crate::core::{AdvisoryData, Destination};
        let destination = Destination::queue("orders");
        let advisory = AdvisoryData::ConsumerAdded {
            connection: 1,
            consumer: 3,
            external_id: Some("ID:client:2:3".into()),
            destination: destination.clone(),
            selector: Some("price > 1".into()),
            durable_name: None,
            client_id: Some("client".into()),
            network: true,
            broker_path: vec!["origin".into()],
            prefetch: 25,
        };
        for version in 1..=12 {
            let mut payload = Vec::new();
            encode_advisory(&mut payload, &advisory, version);
            assert_eq!(payload[..2], [1, 5]);
            let decoded = decode_consumer_version(
                &OpenWireFrame {
                    command_type: 5,
                    payload: payload[2..].to_vec(),
                },
                version,
            )
            .unwrap();
            assert_eq!(decoded.destination, wire_destination(&destination));
            assert_eq!(decoded.prefetch, 25);
            assert_eq!(decoded.selector.as_deref(), Some("price > 1"));
            assert!(!decoded.retroactive);
            assert!(decoded.network);
            assert_eq!(decoded.broker_path, ["origin"]);
            payload.push(0xa5);
            let mut decoder = Decoder::new(&payload);
            decoder.nested_version(version).unwrap();
            assert_eq!(decoder.u8().unwrap(), 0xa5, "version {version}");
        }
    }

    #[test]
    fn destination_info_layout_is_unchanged_in_legacy_versions() {
        let connection = WireId {
            connection: "ID:client".into(),
            session: 0,
            value: 0,
        };
        let destination = WireDestination {
            kind: 100,
            name: "orders".into(),
        };
        for version in 1..=12 {
            let frame = client_destination_info(7, &connection, &destination, 1);
            let (_, decoded, operation) = decode_destination_info(&frame, version).unwrap();
            assert_eq!(decoded, Some(destination.clone()));
            assert_eq!(operation, 1);
        }
    }

    #[test]
    fn dispatch_preserves_message_identity_across_versions_and_redelivery() {
        use crate::core::{Destination, Message};
        let consumer = WireId {
            connection: "ID:consumer".into(),
            session: 2,
            value: 3,
        };
        let destination = wire_destination(&Destination::queue("orders"));
        for id in ["ID:producer:7:8:9", "ID:native-uuid"] {
            let mut message = Message::text(Destination::queue("orders"), "body");
            message.message_id = id.to_owned();
            for version in 1..=12 {
                for (command_id, redelivery) in [(1, 0), (999, 1)] {
                    let frame = encode_dispatch_version(
                        &consumer,
                        &destination,
                        &message,
                        command_id,
                        redelivery,
                        version,
                    );
                    let (_, _, incoming, actual_redelivery) =
                        decode_dispatch_version(&frame, 1024 * 1024, version).unwrap();
                    assert_eq!(incoming.message_id, dispatched_message_id(id, version));
                    assert_eq!(actual_redelivery, redelivery);
                    if id == "ID:producer:7:8:9" {
                        assert_eq!(incoming.message_id, id);
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn legacy_acknowledgements_consume_opaque_native_message_ids() {
        use super::super::{FrameReader, write_frame};
        use crate::config::{AppConfig, ListenerConfig};
        use crate::core::{AdvisoryData, ConnectInfo, Destination, Message};
        use tokio::net::TcpStream;

        fn command(advisory: &AdvisoryData, version: i32, id: i32) -> OpenWireFrame {
            let mut encoded = Vec::new();
            encode_advisory(&mut encoded, advisory, version);
            let mut payload = encoded[2..].to_vec();
            payload[..5].copy_from_slice(&base(id, true));
            OpenWireFrame {
                command_type: encoded[1],
                payload,
            }
        }

        async fn receive(stream: &mut TcpStream, reader: &mut FrameReader) -> OpenWireFrame {
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                loop {
                    let frame = reader.read_frame(stream, 1024 * 1024).await.unwrap();
                    if frame.command_type != 10 {
                        return frame;
                    }
                }
            })
            .await
            .unwrap()
        }

        async fn exchange(
            stream: &mut TcpStream,
            reader: &mut FrameReader,
            frame: &OpenWireFrame,
        ) -> OpenWireFrame {
            write_frame(stream, frame, 1024 * 1024).await.unwrap();
            receive(stream, reader).await
        }

        for version in [1, 9, 12] {
            let mut config = AppConfig::default();
            config.protocols.openwire = ListenerConfig::new(true, "127.0.0.1:0");
            config.protocols.stomp.enabled = false;
            config.protocols.amqp.enabled = false;
            config.protocols.mqtt.enabled = false;
            config.persistence.backend = "memory".into();
            config.web_console.enabled = false;
            config.policy.advisory_support = false;
            let broker = crate::runtime::start(config).await.unwrap();
            let mut stream = TcpStream::connect(broker.address("openwire").unwrap())
                .await
                .unwrap();
            let mut reader = FrameReader::default();
            assert_eq!(
                exchange(
                    &mut stream,
                    &mut reader,
                    &encode_wire_format_info("test", 1024 * 1024, version),
                )
                .await
                .command_type,
                1
            );
            assert_eq!(
                reader
                    .read_frame(&mut stream, 1024 * 1024)
                    .await
                    .unwrap()
                    .command_type,
                2
            );
            let connection = command(
                &AdvisoryData::ConnectionAdded {
                    connection: 17,
                    client_id: Some("native-id-consumer".into()),
                    username: None,
                    remote_address: "test".into(),
                },
                version,
                1,
            );
            assert_eq!(
                exchange(&mut stream, &mut reader, &connection)
                    .await
                    .command_type,
                30
            );
            let destination = Destination::queue("native-message-ids");
            let consumer = command(
                &AdvisoryData::ConsumerAdded {
                    connection: 17,
                    consumer: 1,
                    external_id: Some("rust-17:1:1".into()),
                    destination: destination.clone(),
                    selector: None,
                    durable_name: None,
                    client_id: None,
                    network: false,
                    broker_path: Vec::new(),
                    prefetch: 10,
                },
                version,
                2,
            );
            assert_eq!(
                exchange(&mut stream, &mut reader, &consumer)
                    .await
                    .command_type,
                30
            );
            let (publisher, _) = broker.core.connect(ConnectInfo::default()).unwrap();
            for ack_type in [2, 4] {
                let message = Message::text(destination.clone(), "native message");
                let (producer, sequence) = dispatch_identity(&message.message_id);
                broker.core.send(publisher, message, None).unwrap();
                let frame = receive(&mut stream, &mut reader).await;
                let (consumer, wire_destination, incoming, _) =
                    decode_dispatch_version(&frame, 1024 * 1024, version).unwrap();
                assert_eq!(broker.core.consumers()[0].in_flight, 1);
                let mut payload = base(3, true);
                nested_destination(&mut payload, &wire_destination);
                payload.push(0);
                nested_id(&mut payload, 122, &consumer);
                payload.extend_from_slice(&[ack_type, 0, 1, 110]);
                if version >= 10 {
                    write_string(&mut payload, Some(&incoming.message_id)).unwrap();
                }
                nested_id(&mut payload, 123, &producer);
                payload.extend_from_slice(&sequence.to_be_bytes());
                payload.extend_from_slice(&0i64.to_be_bytes());
                payload.extend_from_slice(&1i32.to_be_bytes());
                if version >= 7 {
                    payload.push(0);
                }
                let ack = OpenWireFrame {
                    command_type: 22,
                    payload,
                };
                assert_eq!(
                    exchange(&mut stream, &mut reader, &ack).await.command_type,
                    30
                );
                assert_eq!(broker.core.consumers()[0].in_flight, 0);
            }
            broker.stop();
        }
    }

    #[test]
    fn client_consumer_and_producer_commands_round_trip() {
        let id = WireId {
            connection: "ID:client".into(),
            session: 2,
            value: 3,
        };
        let destination = WireDestination {
            kind: 100,
            name: "orders".into(),
        };
        let consumer = client_consumer_info(7, &id, &destination, 25, Some("price > 1"));
        let decoded = decode_consumer(&consumer).unwrap();
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.destination, destination);
        assert_eq!(decoded.prefetch, 25);
        assert_eq!(decoded.selector.as_deref(), Some("price > 1"));

        let producer = client_producer_info(8, &id, Some(&destination));
        let decoded = decode_producer(&producer).unwrap();
        assert_eq!(decoded.id, id);
        assert_eq!(decoded.destination, Some(destination));
    }

    #[test]
    fn client_message_and_ack_commands_round_trip() {
        use crate::core::{Destination, DestinationKind, Message, Value};
        let id = WireId {
            connection: "ID:client".into(),
            session: 2,
            value: 3,
        };
        let mut message =
            Message::text(Destination::new(DestinationKind::Queue, "orders"), "hello");
        message.group_id = Some("group-1".into());
        message.properties.insert("attempt".into(), Value::Int(4));
        let frame = client_message(9, &id, &message);
        let decoded = decode_message(&frame, 1024 * 1024).unwrap();
        assert_eq!(decoded.message_id, message.message_id);
        assert_eq!(decoded.group_id.as_deref(), Some("group-1"));
        assert_eq!(decode_text_body(&decoded.body).unwrap(), "hello");
        assert_eq!(
            unmarshal_properties(&decoded.properties)
                .unwrap()
                .get("attempt"),
            Some(&Value::Int(4))
        );

        let ack = client_ack(
            10,
            &id,
            &wire_destination(&message.destination),
            &message.message_id,
        );
        let decoded = decode_ack(&ack).unwrap();
        assert_eq!(decoded.consumer, id);
        assert_eq!(decoded.ack_type, 2);
        assert_eq!(
            decoded.message_id.as_deref(),
            Some(message.message_id.as_str())
        );
        assert_eq!(decoded.count, 1);
    }

    #[test]
    fn advisory_data_structure_round_trips_network_consumer_identity() {
        use crate::core::{AdvisoryData, Destination, DestinationKind, Message};
        let producer = WireId {
            connection: "ID:bridge".into(),
            session: 1,
            value: 1,
        };
        let mut advisory_message = Message::text(
            Destination::topic("ActiveMQ.Advisory.Consumer.Queue.orders"),
            "",
        );
        advisory_message.advisory = Some(AdvisoryData::ConsumerAdded {
            connection: 19,
            consumer: 23,
            external_id: Some("ID:bridge:1:23".into()),
            destination: Destination::new(DestinationKind::Queue, "orders"),
            selector: Some("region = 'west'".into()),
            durable_name: Some("durable-orders".into()),
            client_id: Some("bridge-client".into()),
            network: true,
            broker_path: vec!["origin".into(), "bridge".into()],
            prefetch: 32,
        });

        let encoded = client_message(1, &producer, &advisory_message);
        let decoded = decode_message(&encoded, 1024 * 1024).unwrap();
        let Some(crate::core::AdvisoryData::ConsumerAdded {
            external_id,
            destination,
            selector,
            durable_name,
            client_id,
            network,
            broker_path,
            prefetch,
            ..
        }) = decoded.advisory
        else {
            panic!("expected ConsumerInfo advisory data");
        };
        assert_eq!(external_id.as_deref(), Some("ID:bridge:1:23"));
        assert_eq!(destination.name, "orders");
        assert_eq!(selector.as_deref(), Some("region = 'west'"));
        assert_eq!(durable_name.as_deref(), Some("durable-orders"));
        assert_eq!(client_id.as_deref(), Some("bridge-client"));
        assert!(network);
        assert_eq!(broker_path, ["origin", "bridge"]);
        assert_eq!(prefetch, 32);
    }
}
