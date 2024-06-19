/// Given a maximum value, determine the appropriate byte width to encode with.
pub fn determine_byte_width(max_value: usize) -> u8 {
    if max_value <= i8::MAX as usize {
        1
    } else if max_value <= i16::MAX as usize {
        2
    } else if max_value <= i32::MAX as usize {
        4
    } else {
        8
    }
}

/// Write an integer to a buffer with a specific byte width.
pub fn write_integer(buffer: &mut Vec<u8>, value: usize, byte_width: u8) {
    match byte_width {
        1 => buffer.extend_from_slice(&(value as i8).to_le_bytes()),
        2 => buffer.extend_from_slice(&(value as i16).to_le_bytes()),
        4 => buffer.extend_from_slice(&(value as i32).to_le_bytes()),
        8 => buffer.extend_from_slice(&(value as i64).to_le_bytes()),
        _ => unreachable!(),
    };
}
