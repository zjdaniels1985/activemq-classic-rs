use crate::config::WebConsoleConfig;
use crate::openwire::OpenWireFrame;
use crate::openwire_commands::{
    ConsumerId, IncomingMessage, decode_ack, decode_consumer_info, decode_message,
    decode_remove_consumer, encode_dispatch,
};
use crate::sled_store::SledStore;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Cursor, Read};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;
const OPENWIRE_VERSION: i32 = 12;

pub(crate) type SharedBroker = Arc<Mutex<BrokerState>>;

pub(crate) struct BrokerState {
    pub(crate) queues: HashMap<String, VecDeque<IncomingMessage>>,
    pub(crate) store: SledStore,
}

impl BrokerState {
    fn open(store_path: &std::path::Path) -> io::Result<Self> {
        let store = SledStore::open(store_path)?;
        let mut queues = HashMap::<String, VecDeque<IncomingMessage>>::new();
        for message in store.recover()? {
            queues
                .entry(message.destination.clone())
                .or_default()
                .push_back(message.clone());
        }
        Ok(Self { queues, store })
    }

    fn send(&mut self, message: IncomingMessage) -> io::Result<()> {
        self.store.save(&message)?;
        self.queues
            .entry(message.destination.clone())
            .or_default()
            .push_back(message);
        Ok(())
    }

    fn acknowledge(&mut self, messages: &[IncomingMessage]) -> io::Result<()> {
        for message in messages {
            self.store.acknowledge(message)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind_address: String,
    pub max_frame_size: usize,
    pub broker_name: String,
    pub store_path: PathBuf,
    pub web_console: WebConsoleConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:61616".to_owned(),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            broker_name: "activemq-classic-rs".to_owned(),
            store_path: PathBuf::from("./data/sled"),
            web_console: WebConsoleConfig::default(),
        }
    }
}

pub fn serve(config: ServerConfig) -> io::Result<()> {
    if config.max_frame_size == 0 || config.max_frame_size > i32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "max frame size must be between 1 and i32::MAX",
        ));
    }
    let address = config
        .bind_address
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid bind address"))?;
    let listener = TcpListener::bind(address)?;
    tracing::info!(address = %listener.local_addr()?, "OpenWire listener ready");
    let broker = Arc::new(Mutex::new(BrokerState::open(&config.store_path)?));
    if config.web_console.enabled {
        crate::web_console::start(Arc::clone(&broker), config.web_console.clone())?;
    }
    let broker_sequence = Arc::new(AtomicI64::new(0));

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let max_frame_size = config.max_frame_size;
                let broker = Arc::clone(&broker);
                let broker_sequence = Arc::clone(&broker_sequence);
                let broker_name = config.broker_name.clone();
                thread::Builder::new()
                    .name("activemq-client".to_owned())
                    .spawn(move || {
                        if let Err(error) = handle_client(
                            stream,
                            max_frame_size,
                            broker,
                            broker_sequence,
                            broker_name,
                        ) {
                            tracing::warn!(%error, "OpenWire client disconnected");
                        }
                    })?;
            }
            Err(error) => tracing::error!(%error, "Failed to accept client connection"),
        }
    }
    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    max_frame_size: usize,
    broker: SharedBroker,
    broker_sequence: Arc<AtomicI64>,
    broker_name: String,
) -> io::Result<()> {
    let wire_format = OpenWireFrame::read_from(&mut stream, max_frame_size)?;
    validate_wire_format_info(&wire_format)?;

    write_wire_format_info(&mut stream, max_frame_size, &broker_name)?;
    write_broker_info(&mut stream, max_frame_size, &broker_name)?;
    stream.set_read_timeout(Some(Duration::from_millis(50)))?;

    let mut buffer = Vec::new();
    let mut consumers = HashMap::<ConsumerId, (String, usize)>::new();
    let mut inflight = HashMap::<ConsumerId, VecDeque<IncomingMessage>>::new();
    let mut chunk = [0; 16 * 1024];
    loop {
        while let Some(frame) = take_frame(&mut buffer, max_frame_size)? {
            match frame.command_type {
                10 | 15 => {}
                11 => {
                    if let Some(command_id) = response_required_command_id(&frame)? {
                        write_response(&mut stream, command_id, max_frame_size)?;
                    }
                    restore_inflight(&broker, &mut inflight)?;
                    return Ok(());
                }
                5 => {
                    let consumer = decode_consumer_info(&frame)?;
                    if !consumer.is_topic {
                        consumers.insert(
                            consumer.consumer_id.clone(),
                            (consumer.destination, consumer.prefetch_size.max(1)),
                        );
                        inflight.entry(consumer.consumer_id.clone()).or_default();
                    }
                    if consumer.response_required {
                        write_response(&mut stream, consumer.command_id, max_frame_size)?;
                    }
                }
                12 => {
                    if let Some(consumer_id) = decode_remove_consumer(&frame)? {
                        consumers.remove(&consumer_id);
                        if let Some(messages) = inflight.remove(&consumer_id) {
                            restore_inflight(
                                &broker,
                                &mut HashMap::from([(consumer_id, messages)]),
                            )?;
                        }
                    }
                    if let Some(command_id) = response_required_command_id(&frame)? {
                        write_response(&mut stream, command_id, max_frame_size)?;
                    }
                }
                22 => {
                    let (consumer_id, message_count) = decode_ack(&frame)?;
                    let mut acknowledged = Vec::new();
                    if let Some(messages) = inflight.get_mut(&consumer_id) {
                        for _ in 0..message_count.max(0) {
                            if let Some(message) = messages.pop_front() {
                                acknowledged.push(message);
                            }
                        }
                    }
                    broker
                        .lock()
                        .map_err(|_| io::Error::other("broker state lock poisoned"))?
                        .acknowledge(&acknowledged)?;
                    if let Some(command_id) = response_required_command_id(&frame)? {
                        write_response(&mut stream, command_id, max_frame_size)?;
                    }
                }
                23..=29 => {
                    let message = decode_message(&frame)?;
                    broker
                        .lock()
                        .map_err(|_| io::Error::other("broker state lock poisoned"))?
                        .send(message.clone())?;
                    if message.response_required {
                        write_response(&mut stream, message.command_id, max_frame_size)?;
                    }
                }
                3..=4 | 6..=9 | 14 | 16..=18 => {
                    if let Some(command_id) = response_required_command_id(&frame)? {
                        write_response(&mut stream, command_id, max_frame_size)?;
                    }
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "OpenWire command type {} is not implemented",
                            frame.command_type
                        ),
                    ));
                }
            }
        }

        let consumer_ids = consumers.keys().cloned().collect::<Vec<_>>();
        for consumer_id in consumer_ids {
            let (destination, prefetch) = consumers.get(&consumer_id).expect("consumer exists");
            let unacked = inflight.entry(consumer_id.clone()).or_default();
            while unacked.len() < *prefetch {
                let message = broker
                    .lock()
                    .map_err(|_| io::Error::other("broker state lock poisoned"))?
                    .queues
                    .get_mut(destination)
                    .and_then(VecDeque::pop_front);
                let Some(message) = message else { break };
                let sequence = broker_sequence.fetch_add(1, Ordering::Relaxed) + 1;
                let dispatch = encode_dispatch(
                    &consumer_id,
                    destination,
                    &message,
                    sequence,
                    max_frame_size,
                )?;
                if let Err(error) = dispatch.write_to(&mut stream, max_frame_size) {
                    unacked.push_front(message);
                    restore_inflight(&broker, &mut inflight)?;
                    return Err(error);
                }
                unacked.push_back(message);
            }
        }

        match stream.read(&mut chunk) {
            Ok(0) => {
                restore_inflight(&broker, &mut inflight)?;
                return Ok(());
            }
            Ok(count) => buffer.extend_from_slice(&chunk[..count]),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => {
                restore_inflight(&broker, &mut inflight)?;
                return Err(error);
            }
        }
        if buffer.len() > max_frame_size + 4 {
            restore_inflight(&broker, &mut inflight)?;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incomplete OpenWire frame exceeds configured maximum",
            ));
        }
    }
}

fn restore_inflight(
    broker: &SharedBroker,
    inflight: &mut HashMap<ConsumerId, VecDeque<IncomingMessage>>,
) -> io::Result<()> {
    let mut broker = broker
        .lock()
        .map_err(|_| io::Error::other("broker state lock poisoned"))?;
    for messages in inflight.drain().map(|(_, messages)| messages) {
        let mut by_destination = HashMap::<String, Vec<IncomingMessage>>::new();
        for message in messages {
            by_destination
                .entry(message.destination.clone())
                .or_default()
                .push(message);
        }
        for (destination, messages) in by_destination {
            let queue = broker.queues.entry(destination).or_default();
            for message in messages.into_iter().rev() {
                queue.push_front(message);
            }
        }
    }
    Ok(())
}

fn take_frame(buffer: &mut Vec<u8>, max_frame_size: usize) -> io::Result<Option<OpenWireFrame>> {
    if buffer.len() < 4 {
        return Ok(None);
    }
    let frame_size = u32::from_be_bytes(buffer[..4].try_into().unwrap()) as usize;
    if frame_size == 0 || frame_size > max_frame_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid or oversized OpenWire frame",
        ));
    }
    if buffer.len() < frame_size + 4 {
        return Ok(None);
    }
    let frame = OpenWireFrame {
        command_type: buffer[4],
        payload: buffer[5..frame_size + 4].to_vec(),
    };
    buffer.drain(..frame_size + 4);
    Ok(Some(frame))
}

fn validate_wire_format_info(frame: &OpenWireFrame) -> io::Result<()> {
    if frame.command_type != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "first OpenWire command must be WireFormatInfo",
        ));
    }

    let mut payload = Cursor::new(&frame.payload);
    let mut magic = [0; 8];
    payload.read_exact(&mut magic)?;
    if &magic != b"ActiveMQ" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid OpenWire magic",
        ));
    }
    let version = read_i32(&mut payload)?;
    if !(1..=OPENWIRE_VERSION).contains(&version) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported OpenWire version {version}"),
        ));
    }
    if read_u8(&mut payload)? != 0 {
        let properties_size = read_i32(&mut payload)?;
        if properties_size < 0 || properties_size as usize > frame.payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid WireFormatInfo properties size",
            ));
        }
        let mut properties = vec![0; properties_size as usize];
        payload.read_exact(&mut properties)?;
    }
    if payload.position() != frame.payload.len() as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected bytes after WireFormatInfo",
        ));
    }
    Ok(())
}

fn write_wire_format_info(
    stream: &mut TcpStream,
    max_frame_size: usize,
    broker_name: &str,
) -> io::Result<()> {
    let properties = wire_format_properties(max_frame_size, broker_name);
    let mut payload = Vec::with_capacity(properties.len() + 20);
    payload.extend_from_slice(b"ActiveMQ");
    payload.extend_from_slice(&OPENWIRE_VERSION.to_be_bytes());
    payload.push(1);
    payload.extend_from_slice(&(properties.len() as i32).to_be_bytes());
    payload.extend_from_slice(&properties);
    OpenWireFrame {
        command_type: 1,
        payload,
    }
    .write_to(stream, max_frame_size)
}

fn wire_format_properties(max_frame_size: usize, broker_name: &str) -> Vec<u8> {
    let entries: [(&str, PropertyValue); 7] = [
        ("CacheEnabled", PropertyValue::Boolean(false)),
        ("TightEncodingEnabled", PropertyValue::Boolean(false)),
        ("StackTraceEnabled", PropertyValue::Boolean(false)),
        ("MaxInactivityDuration", PropertyValue::Long(0)),
        ("MaxInactivityDurationInitalDelay", PropertyValue::Long(0)),
        ("MaxFrameSize", PropertyValue::Long(max_frame_size as i64)),
        (
            "ProviderName",
            PropertyValue::String(broker_name.to_owned()),
        ),
    ];
    let mut output = Vec::new();
    output.extend_from_slice(&(entries.len() as i32).to_be_bytes());
    for (name, value) in entries {
        write_java_utf(&mut output, name).expect("static property names fit in modified UTF-8");
        match value {
            PropertyValue::Boolean(value) => {
                output.push(1);
                output.push(u8::from(value));
            }
            PropertyValue::Long(value) => {
                output.push(6);
                output.extend_from_slice(&value.to_be_bytes());
            }
            PropertyValue::String(value) => {
                output.push(9);
                write_java_utf(&mut output, &value)
                    .expect("static property values fit in modified UTF-8");
            }
        }
    }
    output
}

enum PropertyValue {
    Boolean(bool),
    Long(i64),
    String(String),
}

fn write_broker_info(
    stream: &mut TcpStream,
    max_frame_size: usize,
    broker_name: &str,
) -> io::Result<()> {
    let mut payload = Vec::with_capacity(96);
    payload.extend_from_slice(&0_i32.to_be_bytes());
    payload.push(0);

    payload.push(1);
    payload.push(124);
    write_loose_string(&mut payload, Some(broker_name))?;
    write_loose_string(&mut payload, Some(""))?;
    payload.push(0);
    write_loose_string(&mut payload, Some(broker_name))?;
    payload.extend_from_slice(&[0; 5]);
    payload.extend_from_slice(&0_i64.to_be_bytes());
    write_loose_string(&mut payload, None)?;
    write_loose_string(&mut payload, None)?;

    OpenWireFrame {
        command_type: 2,
        payload,
    }
    .write_to(stream, max_frame_size)
}

fn response_required_command_id(frame: &OpenWireFrame) -> io::Result<Option<i32>> {
    if frame.payload.len() < 5 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated OpenWire command header",
        ));
    }
    let command_id = i32::from_be_bytes(frame.payload[..4].try_into().unwrap());
    Ok((frame.payload[4] != 0).then_some(command_id))
}

fn write_response(
    stream: &mut TcpStream,
    correlation_id: i32,
    max_frame_size: usize,
) -> io::Result<()> {
    let mut payload = Vec::with_capacity(9);
    payload.extend_from_slice(&0_i32.to_be_bytes());
    payload.push(0);
    payload.extend_from_slice(&correlation_id.to_be_bytes());
    OpenWireFrame {
        command_type: 30,
        payload,
    }
    .write_to(stream, max_frame_size)
}

fn read_u8(reader: &mut impl Read) -> io::Result<u8> {
    let mut value = [0];
    reader.read_exact(&mut value)?;
    Ok(value[0])
}

fn read_i32(reader: &mut impl Read) -> io::Result<i32> {
    let mut value = [0; 4];
    reader.read_exact(&mut value)?;
    Ok(i32::from_be_bytes(value))
}

fn write_loose_string(output: &mut Vec<u8>, value: Option<&str>) -> io::Result<()> {
    match value {
        None => output.push(0),
        Some(value) => {
            output.push(1);
            write_java_utf(output, value)?;
        }
    }
    Ok(())
}

fn write_java_utf(output: &mut Vec<u8>, value: &str) -> io::Result<()> {
    let encoded = value
        .encode_utf16()
        .flat_map(|unit| match unit {
            0x0001..=0x007f => vec![unit as u8],
            0x0000..=0x07ff => vec![
                (0xc0 | ((unit >> 6) & 0x1f)) as u8,
                (0x80 | (unit & 0x3f)) as u8,
            ],
            _ => vec![
                (0xe0 | ((unit >> 12) & 0x0f)) as u8,
                (0x80 | ((unit >> 6) & 0x3f)) as u8,
                (0x80 | (unit & 0x3f)) as u8,
            ],
        })
        .collect::<Vec<_>>();
    let length = u16::try_from(encoded.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "modified UTF-8 string too long",
        )
    })?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(&encoded);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn openwire_client_receives_negotiation_and_broker_identity() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let store_path = test_store_path();
        let broker = Arc::new(Mutex::new(BrokerState::open(&store_path).unwrap()));
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_client(
                stream,
                DEFAULT_MAX_FRAME_SIZE,
                broker,
                Arc::new(AtomicI64::new(0)),
                "activemq-classic-rs".to_owned(),
            )
            .unwrap();
        });

        let mut client = TcpStream::connect(address).unwrap();
        write_client_wire_format_info(&mut client).unwrap();

        let wire_info = OpenWireFrame::read_from(&mut client, DEFAULT_MAX_FRAME_SIZE).unwrap();
        assert_eq!(wire_info.command_type, 1);
        assert_eq!(&wire_info.payload[..8], b"ActiveMQ");
        let broker_info = OpenWireFrame::read_from(&mut client, DEFAULT_MAX_FRAME_SIZE).unwrap();
        assert_eq!(broker_info.command_type, 2);
        assert!(
            broker_info
                .payload
                .windows(b"activemq-classic-rs".len())
                .any(|window| window == b"activemq-classic-rs")
        );

        OpenWireFrame {
            command_type: 10,
            payload: vec![0; 5],
        }
        .write_to(&mut client, DEFAULT_MAX_FRAME_SIZE)
        .unwrap();
        let mut connection_info = 42_i32.to_be_bytes().to_vec();
        connection_info.push(1);
        OpenWireFrame {
            command_type: 3,
            payload: connection_info,
        }
        .write_to(&mut client, DEFAULT_MAX_FRAME_SIZE)
        .unwrap();
        let response = OpenWireFrame::read_from(&mut client, DEFAULT_MAX_FRAME_SIZE).unwrap();
        assert_eq!(response.command_type, 30);
        assert_eq!(&response.payload[5..9], &42_i32.to_be_bytes());

        let mut consumer = 7_i32.to_be_bytes().to_vec();
        consumer.push(1);
        consumer.extend_from_slice(&[1, 122]);
        write_loose_string(&mut consumer, Some("client")).unwrap();
        consumer.extend_from_slice(&1_i64.to_be_bytes());
        consumer.extend_from_slice(&1_i64.to_be_bytes());
        consumer.push(0);
        consumer.extend_from_slice(&[1, 100]);
        write_loose_string(&mut consumer, Some("orders")).unwrap();
        consumer.extend_from_slice(&1_i32.to_be_bytes());
        OpenWireFrame {
            command_type: 5,
            payload: consumer,
        }
        .write_to(&mut client, DEFAULT_MAX_FRAME_SIZE)
        .unwrap();
        assert_eq!(
            OpenWireFrame::read_from(&mut client, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .command_type,
            30
        );

        let mut message = 8_i32.to_be_bytes().to_vec();
        message.push(1);
        write_producer_id_for_test(&mut message).unwrap();
        message.extend_from_slice(&[1, 100]);
        write_loose_string(&mut message, Some("orders")).unwrap();
        message.extend_from_slice(&[0, 0, 1, 110, 0]);
        write_producer_id_for_test(&mut message).unwrap();
        message.extend_from_slice(&3_i64.to_be_bytes());
        message.extend_from_slice(&0_i64.to_be_bytes());
        message.extend_from_slice(&[0, 0]);
        message.extend_from_slice(&0_i32.to_be_bytes());
        message.extend_from_slice(&[0, 0]);
        message.extend_from_slice(&0_i64.to_be_bytes());
        message.push(4);
        message.push(0);
        message.extend_from_slice(&123_i64.to_be_bytes());
        message.push(0);
        message.push(1);
        let body = [0, 0, 0, 2, b'o', b'k'];
        message.extend_from_slice(&(body.len() as i32).to_be_bytes());
        message.extend_from_slice(&body);
        OpenWireFrame {
            command_type: 28,
            payload: message,
        }
        .write_to(&mut client, DEFAULT_MAX_FRAME_SIZE)
        .unwrap();
        assert_eq!(
            OpenWireFrame::read_from(&mut client, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .command_type,
            30
        );
        assert_eq!(
            OpenWireFrame::read_from(&mut client, DEFAULT_MAX_FRAME_SIZE)
                .unwrap()
                .command_type,
            21
        );

        drop(client);
        server.join().unwrap();
        std::fs::remove_dir_all(store_path).unwrap();
    }

    #[test]
    fn rejects_invalid_openwire_magic() {
        let frame = OpenWireFrame {
            command_type: 1,
            payload: vec![
                b'N', b'o', b'p', b'e', b'N', b'o', b'p', b'e', 0, 0, 0, 12, 0,
            ],
        };
        assert_eq!(
            validate_wire_format_info(&frame).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    fn write_client_wire_format_info(stream: &mut TcpStream) -> io::Result<()> {
        let properties = [0, 0, 0, 0];
        let mut payload = Vec::new();
        payload.extend_from_slice(b"ActiveMQ");
        payload.extend_from_slice(&OPENWIRE_VERSION.to_be_bytes());
        payload.push(1);
        payload.extend_from_slice(&(properties.len() as i32).to_be_bytes());
        payload.extend_from_slice(&properties);
        OpenWireFrame {
            command_type: 1,
            payload,
        }
        .write_to(stream, DEFAULT_MAX_FRAME_SIZE)
    }

    fn write_producer_id_for_test(output: &mut Vec<u8>) -> io::Result<()> {
        output.extend_from_slice(&[1, 123]);
        write_loose_string(output, Some("client"))?;
        output.extend_from_slice(&1_i64.to_be_bytes());
        output.extend_from_slice(&1_i64.to_be_bytes());
        Ok(())
    }

    fn test_store_path() -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("activemq-classic-rs-server-{nonce}.sled"))
    }
}
