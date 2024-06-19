//! Read and write the metadata part of the variant format.
//!
//! The metadata part of the variant format storage the version and the
//! dictionary of strings. The strings are mostly object keys, but can also be
//! commonly used strings.
//!
//! Use [`build_metadata`] to create the metadata buffer based on the known
//! strings. Use [`MetadataRef`] to read from the metadata buffer.
//!
//! ```rust
//! use open_variant::metadata::{build_metadata, MetadataRef};
//!
//! let metadata: Vec<u8> = build_metadata(["apple", "carrot", "brussel sprouts"].into_iter());
//!
//! let metadata = MetadataRef::new(&metadata);
//! assert_eq!(metadata.version(), 1);
//!
//! // The ids can be looked up based on the string. They are sorted, enabling
//! // binary search to be used.
//! assert_eq!(metadata.find_string("apple"), Some(0));
//! assert_eq!(metadata.find_string("brussel sprouts"), Some(1));
//! assert_eq!(metadata.find_string("carrot"), Some(2));
//! ```

use std::collections::BTreeSet;

/// Build the metadata buffer.
///
/// The metadata buffer is basically the version of the variant format, plus
/// the dictionary of strings.
pub fn build_metadata<'a>(string_iter: impl Iterator<Item = &'a str>) -> Vec<u8> {
    let strings: BTreeSet<&str> = string_iter.collect();
    // https://github.com/apache/spark/tree/master/common/variant#metadata-encoding
    let total_buffer_size = strings.iter().map(|s| s.len()).sum::<usize>();
    // The largest offset is the total buffer size.
    let offset_size = crate::utils::determine_byte_width(total_buffer_size);
    // <header> <dictionary_size> <offsets> <data>
    let mut capacity = 1; // header byte
    capacity += offset_size as usize * (2 + strings.len()); // dictionary_size, n + 1 offsets
    capacity += total_buffer_size; // string buffer
    let mut output = Vec::with_capacity(capacity);

    // Header
    //  7     6  5   4  3             0
    //  +-------+---+---+---------------+
    //  |       |   |   |    version    |
    //  +-------+---+---+---------------+
    //      ^         ^
    //      |         +-- sorted_strings
    //      +-- offset_size_minus_one
    let version: u8 = 1; // version
    let sorted_strings = 1; // Hardcoded to 1 for now, since we always sort
    let offset_size_minus_one = offset_size - 1;
    let header = version | (sorted_strings << 4) | (offset_size_minus_one << 6);
    output.push(header);

    // Dictionary size
    let push_offset = |output: &mut Vec<u8>, offset: usize| match offset_size {
        1 => output.extend_from_slice(&(offset as i8).to_le_bytes()),
        2 => output.extend_from_slice(&(offset as i16).to_le_bytes()),
        4 => output.extend_from_slice(&(offset as i32).to_le_bytes()),
        8 => output.extend_from_slice(&(offset as i64).to_le_bytes()),
        _ => unreachable!(),
    };
    push_offset(&mut output, strings.len());

    // Offsets
    let mut offset = 0;
    push_offset(&mut output, offset); // Always starts with 0
    for s in &strings {
        offset += s.len();
        push_offset(&mut output, offset);
    }

    // String data
    for s in &strings {
        output.extend_from_slice(s.as_bytes());
    }

    output
}

/// A view into the metadata buffer.
pub struct MetadataRef<'a> {
    header: u8,
    offset_size: u8,
    dictionary_len: usize,
    offsets: &'a [u8],
    data: &'a [u8],
}

impl<'a> MetadataRef<'a> {
    /// Create a new metadata reference from the metadata buffer.
    ///
    /// The slice should start where the metadata buffer starts, but it is allowed
    /// to contain more data after.
    pub fn new(data: &'a [u8]) -> Self {
        let header = data[0];
        let offset_size = ((header & 0b1100_0000) >> 6) + 1;
        let dictionary_len = Self::read_integer(data, 1, offset_size);
        let offsets_start = 1 + offset_size as usize;
        let offsets_end = offsets_start + offset_size as usize * (dictionary_len + 1);

        Self {
            header,
            offset_size,
            dictionary_len,
            offsets: &data[offsets_start..offsets_end],
            data: &data[offsets_end..],
        }
    }

    pub fn version(&self) -> u8 {
        self.header & 0b0000_1111
    }

    pub fn sorted_strings(&self) -> bool {
        self.header & 0b0001_0000 != 0
    }

    pub fn dictionary_len(&self) -> usize {
        self.dictionary_len
    }

    fn read_integer(data: &[u8], offset: usize, byte_width: u8) -> usize {
        let end = offset + byte_width as usize;
        let slice = &data[offset..end];
        match byte_width {
            1 => i8::from_le_bytes(slice.try_into().unwrap()) as usize,
            2 => i16::from_le_bytes(slice.try_into().unwrap()) as usize,
            4 => i32::from_le_bytes(slice.try_into().unwrap()) as usize,
            8 => i64::from_le_bytes(slice.try_into().unwrap()) as usize,
            _ => unreachable!(),
        }
    }

    pub fn get_string<'b>(&'b self, id: usize) -> Option<&'a str> {
        if id >= self.dictionary_len {
            return None;
        }
        let offset = Self::read_integer(
            self.offsets,
            id * self.offset_size as usize,
            self.offset_size,
        );
        let next_offset = Self::read_integer(
            self.offsets,
            (id + 1) * self.offset_size as usize,
            self.offset_size,
        );
        let data = &self.data[offset..next_offset];
        Some(std::str::from_utf8(data).expect("Invalid UTF-8"))
    }

    /// Given a string, return the position / id in the dictionary.
    ///
    /// This uses binary search if the strings are sorted.
    ///
    /// If the string is not found, it returns `None`.
    pub fn find_string(&self, value: &str) -> Option<usize> {
        // TODO: support unsorted strings
        assert!(
            self.sorted_strings(),
            "Unsorted strings are not supported yet"
        );
        let dict_size = self.dictionary_len();
        if dict_size == 0 {
            return None;
        }
        let mut left = 0;
        let mut right = dict_size - 1;
        while left <= right {
            let mid = left + (right - left) / 2;
            let mid_str = self.get_string(mid).unwrap();
            match mid_str.cmp(value) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid - 1,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_empty_metadata() {
        let metadata = build_metadata(std::iter::empty());
        let metadata = MetadataRef::new(&metadata);
        assert_eq!(metadata.version(), 1);
        assert!(metadata.sorted_strings());
        assert_eq!(metadata.dictionary_len(), 0);
        assert_eq!(metadata.get_string(0), None);
        assert_eq!(metadata.find_string("a"), None);
    }

    #[test]
    fn test_build_metadata() {
        let mut metadata = build_metadata(vec!["apple", "carrot", "brussel sprouts"].into_iter());

        // Validate we can handle the buffer being larger than needed
        metadata.extend(vec![0; 20]);

        let metadata = MetadataRef::new(&metadata);
        assert_eq!(metadata.version(), 1);
        assert!(metadata.sorted_strings());
        assert_eq!(metadata.dictionary_len(), 3);

        assert_eq!(metadata.get_string(0), Some("apple"));
        assert_eq!(metadata.get_string(1), Some("brussel sprouts"));
        assert_eq!(metadata.get_string(2), Some("carrot"));
        assert_eq!(metadata.get_string(3), None);

        assert_eq!(metadata.find_string("apple"), Some(0));
        assert_eq!(metadata.find_string("brussel sprouts"), Some(1));
        assert_eq!(metadata.find_string("carrot"), Some(2));
        assert_eq!(metadata.find_string("daikon radish"), None);
    }
}
