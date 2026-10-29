use crate::openwire::OpenWireFrame;
use serde::{Deserialize, Serialize};
use std::io::{self, Cursor, Read};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IncomingMessage {
    pub command_id: i32,
    pub response_required: bool,
    pub destination: String,
    pub producer_id: ProducerId,
    pub producer_sequence: i64,
    pub timestamp: i64,
    pub persistent: bool,
    pub priority: u8,
    pub text_message: bool,
    pub content: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ProducerId {
    pub connection_id: String,
    pub session_id: i64,
    pub value: i64,
}

#[derive(Clone, Debug)]
pub struct ConsumerInfo {
    pub command_id: i32,
    pub response_required: bool,
    pub consumer_id: ConsumerId,
    pub destination: String,
    pub is_topic: bool,
    pub prefetch_size: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConsumerId {
    pub connection_id: String,
    pub session_id: i64,
    pub value: i64,
}

pub fn command_header(frame: &OpenWireFrame) -> io::Result<(i32, bool, Cursor<&[u8]>)> {
    let mut payload = Cursor::new(frame.payload.as_slice());
    let command_id = read_i32(&mut payload)?;
    let response_required = read_bool(&mut payload)?;
    Ok((command_id, response_required, payload))
}

pub fn decode_consumer_info(frame: &OpenWireFrame) -> io::Result<ConsumerInfo> {
    if frame.command_type != 5 {
        return Err(invalid_data("expected ConsumerInfo"));
    }
    let (command_id, response_required, mut payload) = command_header(frame)?;
    let consumer_id = read_cached_consumer_id(&mut payload)?;
    let _browser = read_bool(&mut payload)?;
    let destination = read_cached_destination(&mut payload)?
        .ok_or_else(|| invalid_data("ConsumerInfo destination must be set"))?;
    let prefetch_size = read_i32(&mut payload)?;
    if prefetch_size < 0 {
        return Err(invalid_data("negative consumer prefetch size"));
    }
    Ok(ConsumerInfo {
        command_id,
        response_required,
        consumer_id,
        destination: destination.name,
        is_topic: destination.is_topic,
        prefetch_size: prefetch_size as usize,
    })
}

pub fn decode_message(frame: &OpenWireFrame) -> io::Result<IncomingMessage> {
    if !matches!(frame.command_type, 23..=29) {
        return Err(invalid_data("expected ActiveMQ message command"));
    }
    let (command_id, response_required, mut payload) = command_header(frame)?;
    let producer_id = read_cached_producer_id(&mut payload)?
        .ok_or_else(|| invalid_data("message ProducerId must be set"))?;
    let destination = read_cached_destination(&mut payload)?
        .ok_or_else(|| invalid_data("message destination must be set"))?;
    if destination.is_topic {
        return Err(invalid_data(
            "publishing to topics is not implemented in the queue path",
        ));
    }
    skip_cached_object(&mut payload)?;
    skip_cached_object(&mut payload)?;
    let _message_id = read_message_id(&mut payload)?;
    skip_cached_object(&mut payload)?;
    let _group_id = read_loose_string(&mut payload)?;
    skip_i32(&mut payload)?;
    let _correlation_id = read_loose_string(&mut payload)?;
    let persistent = read_bool(&mut payload)?;
    let _expiration = read_i64(&mut payload)?;
    let priority = read_u8(&mut payload)?;
    skip_nested_object(&mut payload)?;
    let timestamp = read_i64(&mut payload)?;
    let _type = read_loose_string(&mut payload)?;
    let content = read_byte_sequence(&mut payload)?.unwrap_or_default();

    Ok(IncomingMessage {
        command_id,
        response_required,
        destination: destination.name,
        producer_id,
        producer_sequence: _message_id.1,
        timestamp,
        persistent,
        priority,
        text_message: frame.command_type == 28,
        content,
    })
}

pub fn decode_ack(frame: &OpenWireFrame) -> io::Result<(ConsumerId, i32)> {
    if frame.command_type != 22 {
        return Err(invalid_data("expected MessageAck"));
    }
    let (_, _, mut payload) = command_header(frame)?;
    skip_cached_object(&mut payload)?;
    skip_cached_object(&mut payload)?;
    let consumer_id = read_cached_consumer_id(&mut payload)?;
    let _ack_type = read_u8(&mut payload)?;
    skip_nested_object(&mut payload)?;
    skip_nested_object(&mut payload)?;
    let message_count = read_i32(&mut payload)?;
    Ok((consumer_id, message_count))
}

pub fn decode_remove_consumer(frame: &OpenWireFrame) -> io::Result<Option<ConsumerId>> {
    let (_, _, mut payload) = command_header(frame)?;
    match frame.command_type {
        12 => {
            if !read_bool(&mut payload)? {
                return Ok(None);
            }
            let kind = read_u8(&mut payload)?;
            match kind {
                122 => {
                    let consumer_id = ConsumerId {
                        connection_id: read_loose_string(&mut payload)?
                            .ok_or_else(|| invalid_data("ConsumerId connection ID is null"))?,
                        session_id: read_i64(&mut payload)?,
                        value: read_i64(&mut payload)?,
                    };
                    Ok(Some(consumer_id))
                }
                120 => {
                    let _connection_id = read_loose_string(&mut payload)?;
                    Ok(None)
                }
                121 => {
                    let _connection_id = read_loose_string(&mut payload)?;
                    skip_i64(&mut payload)?;
                    Ok(None)
                }
                123 => {
                    let _connection_id = read_loose_string(&mut payload)?;
                    skip_i64(&mut payload)?;
                    skip_i64(&mut payload)?;
                    Ok(None)
                }
                _ => Err(invalid_data("unsupported RemoveInfo object ID")),
            }
        }
        _ => Err(invalid_data("expected RemoveInfo")),
    }
}

pub fn encode_dispatch(
    consumer_id: &ConsumerId,
    destination: &str,
    message: &IncomingMessage,
    broker_sequence: i64,
    max_frame_size: usize,
) -> io::Result<OpenWireFrame> {
    let mut payload = Vec::new();
    write_i32(&mut payload, 0);
    payload.push(0);
    write_cached_consumer_id(&mut payload, consumer_id)?;
    write_cached_destination(&mut payload, destination)?;

    payload.push(1);
    payload.push(if message.text_message { 28 } else { 23 });
    write_message_body(&mut payload, destination, message, broker_sequence)?;
    write_i32(&mut payload, 0);

    let frame = OpenWireFrame {
        command_type: 21,
        payload,
    };
    if frame.payload.len() + 1 > max_frame_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dispatch frame exceeds configured maximum",
        ));
    }
    Ok(frame)
}

fn write_message_body(
    payload: &mut Vec<u8>,
    destination: &str,
    message: &IncomingMessage,
    broker_sequence: i64,
) -> io::Result<()> {
    write_i32(payload, 0);
    payload.push(0);
    write_cached_producer_id(payload, &message.producer_id)?;
    write_cached_destination(payload, destination)?;
    payload.push(0);
    payload.push(0);

    payload.push(1);
    payload.push(110);
    write_loose_string(payload, None)?;
    write_cached_producer_id(payload, &message.producer_id)?;
    write_i64(payload, message.producer_sequence);
    write_i64(payload, broker_sequence);

    payload.push(0);
    write_loose_string(payload, None)?;
    write_i32(payload, 0);
    write_loose_string(payload, None)?;
    payload.push(u8::from(message.persistent));
    write_i64(payload, 0);
    payload.push(message.priority);
    payload.push(0);
    write_i64(payload, message.timestamp);
    write_loose_string(payload, None)?;
    write_byte_sequence(payload, Some(&message.content))?;
    write_byte_sequence(payload, None)?;
    payload.push(0);
    payload.push(0);
    payload.push(0);
    write_i32(payload, 0);
    payload.push(0);
    write_i64(payload, 0);
    write_loose_string(payload, None)?;
    payload.push(0);
    payload.push(0);
    payload.push(0);
    write_i64(payload, 0);
    write_i64(payload, 0);
    payload.push(0);
    Ok(())
}

fn read_cached_consumer_id(reader: &mut impl Read) -> io::Result<ConsumerId> {
    read_nested_consumer_id(reader)
}

fn read_nested_consumer_id(reader: &mut impl Read) -> io::Result<ConsumerId> {
    if !read_bool(reader)? {
        return Err(invalid_data("ConsumerId is null"));
    }
    if read_u8(reader)? != 122 {
        return Err(invalid_data("expected ConsumerId"));
    }
    Ok(ConsumerId {
        connection_id: read_loose_string(reader)?
            .ok_or_else(|| invalid_data("ConsumerId connection ID is null"))?,
        session_id: read_i64(reader)?,
        value: read_i64(reader)?,
    })
}

fn read_cached_producer_id(reader: &mut impl Read) -> io::Result<Option<ProducerId>> {
    if !read_bool(reader)? {
        return Ok(None);
    }
    if read_u8(reader)? != 123 {
        return Err(invalid_data("expected ProducerId"));
    }
    Ok(Some(ProducerId {
        connection_id: read_loose_string(reader)?
            .ok_or_else(|| invalid_data("ProducerId connection ID is null"))?,
        value: read_i64(reader)?,
        session_id: read_i64(reader)?,
    }))
}

struct Destination {
    name: String,
    is_topic: bool,
}

fn read_cached_destination(reader: &mut impl Read) -> io::Result<Option<Destination>> {
    if !read_bool(reader)? {
        return Ok(None);
    }
    let kind = read_u8(reader)?;
    if !matches!(kind, 100 | 101) {
        return Err(invalid_data("expected queue or topic destination"));
    }
    let name =
        read_loose_string(reader)?.ok_or_else(|| invalid_data("destination name is null"))?;
    Ok(Some(Destination {
        name,
        is_topic: kind == 101,
    }))
}

fn read_message_id(reader: &mut impl Read) -> io::Result<(ProducerId, i64)> {
    if !read_bool(reader)? || read_u8(reader)? != 110 {
        return Err(invalid_data("expected MessageId"));
    }
    let _text_view = read_loose_string(reader)?;
    let producer_id = read_cached_producer_id(reader)?
        .ok_or_else(|| invalid_data("MessageId ProducerId is null"))?;
    let sequence = read_i64(reader)?;
    skip_i64(reader)?;
    Ok((producer_id, sequence))
}

fn skip_cached_object(reader: &mut impl Read) -> io::Result<()> {
    skip_nested_object(reader)
}

fn skip_nested_object(reader: &mut impl Read) -> io::Result<()> {
    if !read_bool(reader)? {
        return Ok(());
    }
    let kind = read_u8(reader)?;
    match kind {
        100 | 101 => {
            let _name = read_loose_string(reader)?;
            Ok(())
        }
        110 => {
            let _text_view = read_loose_string(reader)?;
            skip_nested_object(reader)?;
            skip_i64(reader)?;
            skip_i64(reader)
        }
        120 => {
            let _connection_id = read_loose_string(reader)?;
            Ok(())
        }
        121 => {
            let _connection_id = read_loose_string(reader)?;
            skip_i64(reader)
        }
        122 => {
            let _connection_id = read_loose_string(reader)?;
            skip_i64(reader)?;
            skip_i64(reader)
        }
        123 => {
            let _connection_id = read_loose_string(reader)?;
            skip_i64(reader)?;
            skip_i64(reader)
        }
        _ => Err(invalid_data("unsupported nested OpenWire object")),
    }
}

fn write_cached_consumer_id(payload: &mut Vec<u8>, id: &ConsumerId) -> io::Result<()> {
    write_nested_consumer_id(payload, id)
}

fn write_nested_consumer_id(payload: &mut Vec<u8>, id: &ConsumerId) -> io::Result<()> {
    payload.push(1);
    payload.push(122);
    write_loose_string(payload, Some(&id.connection_id))?;
    write_i64(payload, id.session_id);
    write_i64(payload, id.value);
    Ok(())
}

fn write_cached_producer_id(payload: &mut Vec<u8>, id: &ProducerId) -> io::Result<()> {
    payload.push(1);
    payload.push(123);
    write_loose_string(payload, Some(&id.connection_id))?;
    write_i64(payload, id.value);
    write_i64(payload, id.session_id);
    Ok(())
}

fn write_cached_destination(payload: &mut Vec<u8>, name: &str) -> io::Result<()> {
    payload.push(1);
    payload.push(100);
    write_loose_string(payload, Some(name))
}

fn read_byte_sequence(reader: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
    if !read_bool(reader)? {
        return Ok(None);
    }
    let size = read_i32(reader)?;
    if size < 0 || size as usize > 100 * 1024 * 1024 {
        return Err(invalid_data("invalid OpenWire byte sequence size"));
    }
    let mut bytes = vec![0; size as usize];
    reader.read_exact(&mut bytes)?;
    Ok(Some(bytes))
}

fn write_byte_sequence(payload: &mut Vec<u8>, bytes: Option<&[u8]>) -> io::Result<()> {
    match bytes {
        None => payload.push(0),
        Some(bytes) => {
            payload.push(1);
            let size = i32::try_from(bytes.len())
                .map_err(|_| invalid_data("OpenWire byte sequence is too large"))?;
            write_i32(payload, size);
            payload.extend_from_slice(bytes);
        }
    }
    Ok(())
}

fn read_loose_string(reader: &mut impl Read) -> io::Result<Option<String>> {
    if !read_bool(reader)? {
        return Ok(None);
    }
    let length = read_u16(reader)? as usize;
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes)?;
    Ok(Some(String::from_utf8_lossy(&bytes).into_owned()))
}

fn write_loose_string(payload: &mut Vec<u8>, value: Option<&str>) -> io::Result<()> {
    match value {
        None => payload.push(0),
        Some(value) => {
            payload.push(1);
            let length = u16::try_from(value.len())
                .map_err(|_| invalid_data("OpenWire string is too long"))?;
            payload.extend_from_slice(&length.to_be_bytes());
            payload.extend_from_slice(value.as_bytes());
        }
    }
    Ok(())
}

fn read_bool(reader: &mut impl Read) -> io::Result<bool> {
    Ok(read_u8(reader)? != 0)
}

fn read_u8(reader: &mut impl Read) -> io::Result<u8> {
    let mut value = [0];
    reader.read_exact(&mut value)?;
    Ok(value[0])
}

fn read_u16(reader: &mut impl Read) -> io::Result<u16> {
    let mut value = [0; 2];
    reader.read_exact(&mut value)?;
    Ok(u16::from_be_bytes(value))
}

fn read_i32(reader: &mut impl Read) -> io::Result<i32> {
    let mut value = [0; 4];
    reader.read_exact(&mut value)?;
    Ok(i32::from_be_bytes(value))
}

fn read_i64(reader: &mut impl Read) -> io::Result<i64> {
    let mut value = [0; 8];
    reader.read_exact(&mut value)?;
    Ok(i64::from_be_bytes(value))
}

fn write_i32(output: &mut Vec<u8>, value: i32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn write_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn skip_i32(reader: &mut impl Read) -> io::Result<()> {
    let _ = read_i32(reader)?;
    Ok(())
}

fn skip_i64(reader: &mut impl Read) -> io::Result<()> {
    let _ = read_i64(reader)?;
    Ok(())
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_string(output: &mut Vec<u8>, value: Option<&str>) {
        match value {
            None => output.push(0),
            Some(value) => {
                output.push(1);
                output.extend_from_slice(&(value.len() as u16).to_be_bytes());
                output.extend_from_slice(value.as_bytes());
            }
        }
    }

    fn write_producer_id(output: &mut Vec<u8>) {
        output.extend_from_slice(&[1, 123]);
        write_string(output, Some("conn"));
        output.extend_from_slice(&3_i64.to_be_bytes());
        output.extend_from_slice(&2_i64.to_be_bytes());
    }

    fn write_destination(output: &mut Vec<u8>) {
        output.extend_from_slice(&[1, 100]);
        write_string(output, Some("orders"));
    }

    #[test]
    fn encodes_dispatch_as_an_openwire_command() {
        let consumer = ConsumerId {
            connection_id: "conn".to_owned(),
            session_id: 1,
            value: 2,
        };
        let message = IncomingMessage {
            command_id: 7,
            response_required: false,
            destination: "orders".to_owned(),
            producer_id: ProducerId {
                connection_id: "conn".to_owned(),
                session_id: 1,
                value: 0,
            },
            producer_sequence: 4,
            timestamp: 5,
            persistent: false,
            priority: 4,
            text_message: true,
            content: vec![0, 0, 0, 2, b'o', b'k'],
        };

        let frame = encode_dispatch(&consumer, "orders", &message, 9, 1024).unwrap();
        assert_eq!(frame.command_type, 21);
        assert!(frame.payload.len() > message.content.len());
    }

    #[test]
    fn decodes_text_message_and_consumer_commands() {
        let mut consumer = vec![0; 5];
        consumer.extend_from_slice(&[1, 122]);
        write_string(&mut consumer, Some("conn"));
        consumer.extend_from_slice(&2_i64.to_be_bytes());
        consumer.extend_from_slice(&9_i64.to_be_bytes());
        consumer.push(0);
        consumer.extend_from_slice(&[1, 100]);
        write_string(&mut consumer, Some("orders"));
        consumer.extend_from_slice(&10_i32.to_be_bytes());
        let consumer = decode_consumer_info(&OpenWireFrame {
            command_type: 5,
            payload: consumer,
        })
        .unwrap();
        assert_eq!(consumer.destination, "orders");
        assert_eq!(consumer.prefetch_size, 10);
        assert_eq!(consumer.consumer_id.value, 9);

        let mut payload = 77_i32.to_be_bytes().to_vec();
        payload.push(1);
        write_producer_id(&mut payload);
        write_destination(&mut payload);
        payload.extend_from_slice(&[0, 0]);
        payload.extend_from_slice(&[1, 110, 0]);
        write_producer_id(&mut payload);
        payload.extend_from_slice(&4_i64.to_be_bytes());
        payload.extend_from_slice(&0_i64.to_be_bytes());
        payload.push(0);
        payload.push(0);
        payload.extend_from_slice(&0_i32.to_be_bytes());
        payload.push(0);
        payload.push(0);
        payload.extend_from_slice(&0_i64.to_be_bytes());
        payload.push(4);
        payload.push(0);
        payload.extend_from_slice(&123_i64.to_be_bytes());
        payload.push(0);
        payload.push(1);
        let text_content = [0, 0, 0, 2, b'o', b'k'];
        payload.extend_from_slice(&(text_content.len() as i32).to_be_bytes());
        payload.extend_from_slice(&text_content);

        let message = decode_message(&OpenWireFrame {
            command_type: 28,
            payload,
        })
        .unwrap();
        assert_eq!(message.command_id, 77);
        assert!(message.response_required);
        assert_eq!(message.destination, "orders");
        assert_eq!(message.producer_sequence, 4);
        assert_eq!(message.content, text_content);
        assert!(message.text_message);
    }

    #[test]
    fn decodes_topic_consumers_for_advisory_compatibility() {
        let mut frame = OpenWireFrame {
            command_type: 5,
            payload: vec![0; 5],
        };
        frame.payload.extend_from_slice(&[1, 122, 1, 0, 1, b'c']);
        frame.payload.extend_from_slice(&1_i64.to_be_bytes());
        frame.payload.extend_from_slice(&2_i64.to_be_bytes());
        frame.payload.push(0);
        frame.payload.extend_from_slice(&[1, 101, 1, 0, 1, b't']);
        frame.payload.extend_from_slice(&[0; 8]);

        assert!(decode_consumer_info(&frame).unwrap().is_topic);
    }
}
