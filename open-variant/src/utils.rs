pub fn get_offset_size(max_value: usize) -> u8 {
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

pub fn push_offset(buffer: &mut Vec<u8>, offset: usize, offset_size: u8) {
    match offset_size {
        1 => buffer.extend_from_slice(&(offset as i8).to_le_bytes()),
        2 => buffer.extend_from_slice(&(offset as i16).to_le_bytes()),
        4 => buffer.extend_from_slice(&(offset as i32).to_le_bytes()),
        8 => buffer.extend_from_slice(&(offset as i64).to_le_bytes()),
        _ => unreachable!(),
    };
}
