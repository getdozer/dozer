use std::{
    borrow::Cow,
    io::{Error, ErrorKind},
};

use encoding_rs::Encoding;

/// Attempts to detect the character encoding of the provided bytes.
///
/// Supports UTF-8, UTF-16 Little Endian and UTF-16 Big Endian.
pub fn detect_charset(bytes: &'_ [u8]) -> &'static str {
    const UTF16_LE_BOM: &[u8] = b"\xFF\xFE";
    const UTF16_BE_BOM: &[u8] = b"\xFE\xFF";

    if bytes.starts_with(UTF16_LE_BOM) {
        "utf-16le"
    } else if bytes.starts_with(UTF16_BE_BOM) {
        "utf-16be"
    } else {
        // Assume everything else is utf-8
        "utf-8"
    }
}

/// Attempts to convert the provided bytes to a UTF-8 string.
///
/// Supports all encodings supported by the encoding_rs crate, which includes
/// all encodings specified in the WHATWG Encoding Standard, and only those
/// encodings (see: <https://encoding.spec.whatwg.org/>).
pub fn convert_to_utf8<'a>(bytes: &'a [u8], charset: &'_ str) -> Result<Cow<'a, str>, Error> {
    match Encoding::for_label(charset.as_bytes()) {
        Some(encoding) => encoding
            .decode_without_bom_handling_and_without_replacement(bytes)
            .ok_or_else(|| ErrorKind::InvalidData.into()),
        None => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("Unsupported charset: {charset}"),
        )),
    }
}
