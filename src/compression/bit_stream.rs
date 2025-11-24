//! Bit-level I/O primitives for Gorilla compression

use crate::error::CompressionError;

/// Writer for bit-level operations
pub struct BitWriter {
    buffer: Vec<u8>,
    current_byte: u8,
    bit_position: u8,
}

impl BitWriter {
    /// Create a new bit writer
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            current_byte: 0,
            bit_position: 0,
        }
    }

    /// Write a single bit
    pub fn write_bit(&mut self, bit: bool) {
        if bit {
            self.current_byte |= 1 << (7 - self.bit_position);
        }

        self.bit_position += 1;
        debug_assert!(self.bit_position <= 8, "bit_position overflow");

        if self.bit_position >= 8 {
            self.buffer.push(self.current_byte);
            self.current_byte = 0;
            self.bit_position = 0;
        }
    }

    /// Write multiple bits from a u64 value
    pub fn write_bits(&mut self, value: u64, num_bits: u8) {
        debug_assert!(num_bits <= 64, "Cannot write more than 64 bits");

        for i in (0..num_bits).rev() {
            let bit = (value >> i) & 1 == 1;
            self.write_bit(bit);
        }
    }

    /// Flush remaining bits and return the buffer
    pub fn finish(mut self) -> Vec<u8> {
        if self.bit_position > 0 {
            self.buffer.push(self.current_byte);
        }
        self.buffer
    }

    /// Get current buffer size in bytes
    pub fn len(&self) -> usize {
        let mut len = self.buffer.len();
        if self.bit_position > 0 {
            len += 1;
        }
        len
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty() && self.bit_position == 0
    }
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Reader for bit-level operations
pub struct BitReader<'a> {
    buffer: &'a [u8],
    byte_position: usize,
    bit_position: u8,
}

impl<'a> BitReader<'a> {
    /// Create a new bit reader
    pub fn new(buffer: &'a [u8]) -> Self {
        Self {
            buffer,
            byte_position: 0,
            bit_position: 0,
        }
    }

    /// Read a single bit
    pub fn read_bit(&mut self) -> Result<bool, CompressionError> {
        if self.byte_position >= self.buffer.len() {
            return Err(CompressionError::CorruptedData(
                "Unexpected end of buffer".to_string(),
            ));
        }

        let byte = self.buffer[self.byte_position];
        let bit = (byte >> (7 - self.bit_position)) & 1 == 1;

        self.bit_position += 1;

        if self.bit_position >= 8 {
            self.byte_position += 1;
            self.bit_position = 0;
        }

        Ok(bit)
    }

    /// Read multiple bits into a u64 value
    pub fn read_bits(&mut self, num_bits: u8) -> Result<u64, CompressionError> {
        if num_bits > 64 {
            return Err(CompressionError::InvalidData(
                format!("Cannot read more than 64 bits (requested: {})", num_bits)
            ));
        }

        let mut value: u64 = 0;

        for _ in 0..num_bits {
            value = (value << 1) | (self.read_bit()? as u64);
        }

        Ok(value)
    }

    /// Check if we've reached the end
    pub fn is_at_end(&self) -> bool {
        self.byte_position >= self.buffer.len()
    }

    /// Get current position (for debugging)
    pub fn position(&self) -> (usize, u8) {
        (self.byte_position, self.bit_position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_read_single_bit() {
        let mut writer = BitWriter::new();
        writer.write_bit(true);
        writer.write_bit(false);
        writer.write_bit(true);
        writer.write_bit(true);

        let buffer = writer.finish();
        let mut reader = BitReader::new(&buffer);

        assert!(reader.read_bit().unwrap());
        assert!(!reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
        assert!(reader.read_bit().unwrap());
    }

    #[test]
    fn test_write_read_multiple_bits() {
        let mut writer = BitWriter::new();
        writer.write_bits(0b1010, 4);
        writer.write_bits(0b110011, 6);

        let buffer = writer.finish();
        let mut reader = BitReader::new(&buffer);

        assert_eq!(reader.read_bits(4).unwrap(), 0b1010);
        assert_eq!(reader.read_bits(6).unwrap(), 0b110011);
    }

    #[test]
    fn test_write_64_bits() {
        let mut writer = BitWriter::new();
        let value: u64 = 0x123456789ABCDEF0;
        writer.write_bits(value, 64);

        let buffer = writer.finish();
        let mut reader = BitReader::new(&buffer);

        assert_eq!(reader.read_bits(64).unwrap(), value);
    }

    #[test]
    fn test_mixed_operations() {
        let mut writer = BitWriter::new();

        // Write various bit patterns
        writer.write_bit(true);
        writer.write_bits(0b101, 3);
        writer.write_bits(0xFF, 8);
        writer.write_bit(false);

        let buffer = writer.finish();
        let mut reader = BitReader::new(&buffer);

        // Read them back
        assert!(reader.read_bit().unwrap());
        assert_eq!(reader.read_bits(3).unwrap(), 0b101);
        assert_eq!(reader.read_bits(8).unwrap(), 0xFF);
        assert!(!reader.read_bit().unwrap());
    }

    #[test]
    fn test_byte_alignment() {
        let mut writer = BitWriter::new();

        // Write exactly one byte
        writer.write_bits(0b10101010, 8);

        let buffer = writer.finish();
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0], 0b10101010);
    }

    #[test]
    fn test_partial_byte() {
        let mut writer = BitWriter::new();

        // Write less than one byte
        writer.write_bits(0b1010, 4);

        let buffer = writer.finish();
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0] >> 4, 0b1010);
    }

    #[test]
    fn test_read_past_end() {
        let buffer = vec![0b10101010];
        let mut reader = BitReader::new(&buffer);

        // Read all 8 bits
        reader.read_bits(8).unwrap();

        // Try to read more - should fail
        assert!(reader.read_bit().is_err());
    }
}
