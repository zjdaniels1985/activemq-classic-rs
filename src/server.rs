use crate::openwire::OpenWireFrame;
use std::io::{self, Cursor, Read};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::thread;

const DEFAULT_MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;
const OPENWIRE_VERSION: i32 = 12;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind_address: String,
    pub max_frame_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:61616".to_owned(),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
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
    eprintln!("OpenWire listener ready at {}", listener.local_addr()?);

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let max_frame_size = config.max_frame_size;
                thread::Builder::new()
                    .name("activemq-client".to_owned())
                    .spawn(move || {
                        if let Err(error) = handle_client(stream, max_frame_size) {
                            eprintln!("OpenWire client disconnected: {error}");
                        }
                    })?;
            }
            Err(error) => eprintln!("Failed to accept client connection: {error}"),
        }
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream, max_frame_size: usize) -> io::Result<()> {
    let wire_format = OpenWireFrame::read_from(&mut stream, max_frame_size)?;
    validate_wire_format_info(&wire_format)?;

    write_wire_format_info(&mut stream, max_frame_size)?;
    write_broker_info(&mut stream, max_frame_size)?;

    loop {
        let frame = match OpenWireFrame::read_from(&mut stream, max_frame_size) {
            Ok(frame) => frame,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        };
        match frame.command_type {
            10 | 15 => {}
            11 => return Ok(()),
            3..=9 | 12 | 14 | 16..=18 => {
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
}

fn validate_wire_format_info(frame: &OpenWireFrame) -> io::Result<()> {
    if frame.command_type != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "first OpenWire command must be WireFormatInfo",
        ));
    }

    let mut payload = Cursor::new(&frame.payload);
    let booleans = read_boolean_stream(&mut payload)?;
    let _command_id = read_i32(&mut payload)?;
    let _response_required = boolean_at(&booleans, 0)?;
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
    if boolean_at(&booleans, 1)? {
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

fn write_wire_format_info(stream: &mut TcpStream, max_frame_size: usize) -> io::Result<()> {
    let properties = wire_format_properties(max_frame_size);
    let mut payload = Vec::with_capacity(properties.len() + 20);
    payload.extend_from_slice(&[1, 0b0000_0010]);
    payload.extend_from_slice(&0_i32.to_be_bytes());
    payload.extend_from_slice(b"ActiveMQ");
    payload.extend_from_slice(&OPENWIRE_VERSION.to_be_bytes());
    payload.extend_from_slice(&(properties.len() as i32).to_be_bytes());
    payload.extend_from_slice(&properties);
    OpenWireFrame {
        command_type: 1,
        payload,
    }
    .write_to(stream, max_frame_size)
}

fn wire_format_properties(max_frame_size: usize) -> Vec<u8> {
    let entries: [(&str, PropertyValue); 7] = [
        ("CacheEnabled", PropertyValue::Boolean(false)),
        ("TightEncodingEnabled", PropertyValue::Boolean(false)),
        ("StackTraceEnabled", PropertyValue::Boolean(false)),
        ("MaxInactivityDuration", PropertyValue::Long(0)),
        ("MaxInactivityDurationInitalDelay", PropertyValue::Long(0)),
        ("MaxFrameSize", PropertyValue::Long(max_frame_size as i64)),
        (
            "ProviderName",
            PropertyValue::String("ActiveMQ Classic Rust".to_owned()),
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

fn write_broker_info(stream: &mut TcpStream, max_frame_size: usize) -> io::Result<()> {
    let mut payload = Vec::with_capacity(96);
    payload.extend_from_slice(&0_i32.to_be_bytes());
    payload.push(0);

    payload.push(1);
    payload.push(124);
    write_loose_string(&mut payload, Some("activemq-classic-rs"))?;
    write_loose_string(&mut payload, Some(""))?;
    payload.push(0);
    write_loose_string(&mut payload, Some("activemq-classic-rs"))?;
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

fn read_boolean_stream(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let marker = read_u8(reader)?;
    let count = match marker {
        0xC0 => read_u8(reader)? as usize,
        0x80 => read_u16(reader)? as usize,
        value => value as usize,
    };
    if count > 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "OpenWire boolean stream is too large",
        ));
    }
    let mut values = vec![0; count];
    reader.read_exact(&mut values)?;
    Ok(values)
}

fn boolean_at(values: &[u8], index: usize) -> io::Result<bool> {
    let byte = values.get(index / 8).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated OpenWire boolean stream",
        )
    })?;
    Ok(byte & (1 << (index % 8)) != 0)
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
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_client(stream.try_clone().unwrap(), DEFAULT_MAX_FRAME_SIZE).unwrap();
        });

        let mut client = TcpStream::connect(address).unwrap();
        write_client_wire_format_info(&mut client).unwrap();

        let wire_info = OpenWireFrame::read_from(&mut client, DEFAULT_MAX_FRAME_SIZE).unwrap();
        assert_eq!(wire_info.command_type, 1);
        assert_eq!(&wire_info.payload[6..14], b"ActiveMQ");
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

        drop(client);
        server.join().unwrap();
    }

    #[test]
    fn rejects_invalid_openwire_magic() {
        let frame = OpenWireFrame {
            command_type: 1,
            payload: vec![
                1, 0, 0, 0, 0, 0, b'N', b'o', b'p', b'e', b'N', b'o', b'p', b'e', 0, 0, 0, 12, 0,
            ],
        };
        assert_eq!(
            validate_wire_format_info(&frame).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    fn write_client_wire_format_info(stream: &mut TcpStream) -> io::Result<()> {
        let properties = [0, 0, 0, 0];
        let mut payload = vec![1, 0b0000_0010];
        payload.extend_from_slice(&0_i32.to_be_bytes());
        payload.extend_from_slice(b"ActiveMQ");
        payload.extend_from_slice(&OPENWIRE_VERSION.to_be_bytes());
        payload.extend_from_slice(&(properties.len() as i32).to_be_bytes());
        payload.extend_from_slice(&properties);
        OpenWireFrame {
            command_type: 1,
            payload,
        }
        .write_to(stream, DEFAULT_MAX_FRAME_SIZE)
    }
}
