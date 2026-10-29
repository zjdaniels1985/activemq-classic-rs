use std::io::{self, Read, Write};

/// An OpenWire command frame, excluding the four-byte size prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenWireFrame {
    pub command_type: u8,
    pub payload: Vec<u8>,
}

impl OpenWireFrame {
    pub fn read_from(reader: &mut impl Read, max_frame_size: usize) -> io::Result<Self> {
        let mut size_bytes = [0; 4];
        reader.read_exact(&mut size_bytes)?;
        let frame_size = u32::from_be_bytes(size_bytes) as usize;

        if frame_size == 0 {
            return Err(invalid_data("OpenWire frame must include a command type"));
        }
        if frame_size > max_frame_size {
            return Err(invalid_data("OpenWire frame exceeds configured maximum"));
        }

        let mut body = vec![0; frame_size];
        reader.read_exact(&mut body)?;
        let command_type = body[0];
        let payload = body[1..].to_vec();
        Ok(Self {
            command_type,
            payload,
        })
    }

    pub fn write_to(&self, writer: &mut impl Write, max_frame_size: usize) -> io::Result<()> {
        let frame_size = self
            .payload
            .len()
            .checked_add(1)
            .filter(|size| *size <= max_frame_size && *size <= u32::MAX as usize)
            .ok_or_else(|| invalid_input("OpenWire frame exceeds configured maximum"))?;

        writer.write_all(&(frame_size as u32).to_be_bytes())?;
        writer.write_all(&[self.command_type])?;
        writer.write_all(&self.payload)
    }
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn reads_and_writes_size_prefixed_frames() {
        let frame = OpenWireFrame {
            command_type: 1,
            payload: b"ActiveMQ".to_vec(),
        };
        let mut bytes = Vec::new();
        frame.write_to(&mut bytes, 1024).unwrap();
        assert_eq!(&bytes[..4], &9_u32.to_be_bytes());
        assert_eq!(
            OpenWireFrame::read_from(&mut Cursor::new(bytes), 1024).unwrap(),
            frame
        );
    }

    #[test]
    fn rejects_empty_or_oversized_frames_before_allocating_the_body() {
        let mut empty_frame = Cursor::new(0_u32.to_be_bytes());
        assert_eq!(
            OpenWireFrame::read_from(&mut empty_frame, 1024)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );

        let mut oversized_frame = Cursor::new(5_u32.to_be_bytes());
        assert_eq!(
            OpenWireFrame::read_from(&mut oversized_frame, 4)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn rejects_oversized_frames_when_writing() {
        let frame = OpenWireFrame {
            command_type: 1,
            payload: vec![0; 4],
        };
        assert_eq!(
            frame.write_to(&mut Vec::new(), 4).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
