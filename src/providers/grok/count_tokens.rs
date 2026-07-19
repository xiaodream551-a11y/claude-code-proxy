use std::io::{Cursor, Read as _};

use base64::read::DecoderReader;

use super::translate::request::{GrokContentPart, GrokInputItem, GrokResponsesRequest};

const MESSAGE_OVERHEAD_TOKENS: u64 = 4;
const TOOL_OVERHEAD_TOKENS: u64 = 4;
const IMAGE_OVERHEAD_TOKENS: u64 = 256;
const IMAGE_TILE_EDGE: u64 = 512;
const IMAGE_TILE_TOKENS: u64 = 256;
const IMAGE_FALLBACK_BYTES_PER_TILE: u64 = 256 * 1024;
const MAX_IMAGE_ESTIMATE_TOKENS: u64 = 65_536;
/// Enough for PNG's IHDR and for the usual JPEG APP/EXIF preamble. If a JPEG
/// pushes its SOF marker farther into the file, use decoded byte count instead
/// of retaining the entire image just to estimate tokens.
const IMAGE_METADATA_PREFIX_BYTES: usize = 64 * 1024;
const IMAGE_DECODE_BUFFER_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Base64ImageError {
    Invalid,
    TooSmall,
    TooLarge,
}

pub fn count_tokens(request: &GrokResponsesRequest) -> u64 {
    let instructions = request
        .instructions
        .as_deref()
        .map(approx_token_count)
        .unwrap_or(0);
    let input: u64 = request.input.iter().map(count_input_item).sum();
    let tools: u64 = request
        .tools
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|tool| {
            tool.name.as_deref().map(approx_token_count).unwrap_or(0)
                + tool
                    .description
                    .as_deref()
                    .map(approx_token_count)
                    .unwrap_or(0)
                + approx_token_count(&serde_json::to_string(&tool.parameters).unwrap_or_default())
                + TOOL_OVERHEAD_TOKENS
        })
        .sum();

    (instructions
        + input
        + tools
        + request.input.len() as u64 * MESSAGE_OVERHEAD_TOKENS
        + approx_token_count(&request.model))
    .max(1)
}

fn count_input_item(item: &GrokInputItem) -> u64 {
    match item {
        GrokInputItem::Message { content, .. } => content
            .iter()
            .map(|part| match part {
                GrokContentPart::InputText { text } | GrokContentPart::OutputText { text } => {
                    approx_token_count(text)
                }
                GrokContentPart::InputImage {
                    image_url,
                    estimated_tokens,
                } => estimated_tokens.unwrap_or_else(|| estimate_image_tokens(image_url)),
            })
            .sum(),
        GrokInputItem::FunctionCall {
            name, arguments, ..
        } => approx_token_count(name) + approx_token_count(arguments),
        GrokInputItem::FunctionCallOutput { output, .. } => approx_token_count(output),
    }
}

fn estimate_image_tokens(image_url: &str) -> u64 {
    let Some((metadata, encoded)) = image_url.split_once(',') else {
        return IMAGE_OVERHEAD_TOKENS + approx_token_count(image_url);
    };
    if !metadata.starts_with("data:image/") || !metadata.ends_with(";base64") {
        return IMAGE_OVERHEAD_TOKENS + approx_token_count(image_url);
    }
    validate_and_estimate_base64_image(encoded, usize::MAX).unwrap_or(IMAGE_OVERHEAD_TOKENS)
}

/// Validate an embedded image in one streaming decode pass and cache the
/// estimate at translation time. Memory is bounded by the decoder buffers and
/// [`IMAGE_METADATA_PREFIX_BYTES`], independent of the image's decoded size.
pub(super) fn validate_and_estimate_base64_image(
    encoded: &str,
    max_decoded_bytes: usize,
) -> Result<u64, Base64ImageError> {
    if encoded.len() > max_base64_len(max_decoded_bytes) {
        return Err(Base64ImageError::TooLarge);
    }

    let mut decoder = DecoderReader::new(
        Cursor::new(encoded.as_bytes()),
        &base64::engine::general_purpose::STANDARD,
    );
    let mut buffer = [0_u8; IMAGE_DECODE_BUFFER_BYTES];
    let mut prefix = Vec::with_capacity(IMAGE_METADATA_PREFIX_BYTES.min(max_decoded_bytes));
    let mut decoded_len = 0_usize;

    loop {
        let read = decoder
            .read(&mut buffer)
            .map_err(|_| Base64ImageError::Invalid)?;
        if read == 0 {
            break;
        }
        decoded_len = decoded_len
            .checked_add(read)
            .ok_or(Base64ImageError::TooLarge)?;
        if decoded_len > max_decoded_bytes {
            return Err(Base64ImageError::TooLarge);
        }

        let prefix_remaining = IMAGE_METADATA_PREFIX_BYTES.saturating_sub(prefix.len());
        prefix.extend_from_slice(&buffer[..read.min(prefix_remaining)]);
    }

    if image_dimensions(&prefix).is_some_and(|(width, height)| {
        width < 8 || height < 8 || width.saturating_mul(height) < 512
    }) {
        return Err(Base64ImageError::TooSmall);
    }

    Ok(estimate_image_tokens_from_summary(
        &prefix,
        decoded_len as u64,
    ))
}

fn max_base64_len(max_decoded_bytes: usize) -> usize {
    max_decoded_bytes
        .saturating_add(2)
        .checked_div(3)
        .unwrap_or(0)
        .saturating_mul(4)
}

fn estimate_image_tokens_from_summary(prefix: &[u8], decoded_len: u64) -> u64 {
    let estimate = image_dimensions(prefix)
        .map(|(width, height)| {
            width
                .div_ceil(IMAGE_TILE_EDGE)
                .saturating_mul(height.div_ceil(IMAGE_TILE_EDGE))
                .saturating_mul(IMAGE_TILE_TOKENS)
        })
        .unwrap_or_else(|| {
            decoded_len
                .div_ceil(IMAGE_FALLBACK_BYTES_PER_TILE)
                .saturating_mul(IMAGE_TILE_TOKENS)
        });
    estimate.clamp(IMAGE_OVERHEAD_TOKENS, MAX_IMAGE_ESTIMATE_TOKENS)
}

fn image_dimensions(bytes: &[u8]) -> Option<(u64, u64)> {
    png_dimensions(bytes)
        .or_else(|| jpeg_dimensions(bytes))
        .filter(|(width, height)| *width > 0 && *height > 0)
}

fn png_dimensions(bytes: &[u8]) -> Option<(u64, u64)> {
    if bytes.len() < 24 || !bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return None;
    }
    Some((
        u32::from_be_bytes(bytes[16..20].try_into().ok()?) as u64,
        u32::from_be_bytes(bytes[20..24].try_into().ok()?) as u64,
    ))
}

fn jpeg_dimensions(bytes: &[u8]) -> Option<(u64, u64)> {
    if !bytes.starts_with(&[0xff, 0xd8]) {
        return None;
    }
    let mut offset = 2_usize;
    while offset + 3 < bytes.len() {
        if bytes[offset] != 0xff {
            offset += 1;
            continue;
        }
        while offset < bytes.len() && bytes[offset] == 0xff {
            offset += 1;
        }
        let marker = *bytes.get(offset)?;
        offset += 1;
        if matches!(marker, 0xd8 | 0xd9) {
            continue;
        }
        let length = u16::from_be_bytes(bytes.get(offset..offset + 2)?.try_into().ok()?) as usize;
        if length < 2 || offset + length > bytes.len() {
            return None;
        }
        if matches!(
            marker,
            0xc0 | 0xc1
                | 0xc2
                | 0xc3
                | 0xc5
                | 0xc6
                | 0xc7
                | 0xc9
                | 0xca
                | 0xcb
                | 0xcd
                | 0xce
                | 0xcf
        ) && length >= 7
        {
            let height = u16::from_be_bytes(bytes[offset + 3..offset + 5].try_into().ok()?);
            let width = u16::from_be_bytes(bytes[offset + 5..offset + 7].try_into().ok()?);
            return Some((u64::from(width), u64::from(height)));
        }
        offset += length;
    }
    None
}

fn approx_token_count(text: &str) -> u64 {
    if text.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut in_word = false;
    for character in text.chars() {
        if character.is_alphanumeric() || character == '-' || character == '_' {
            if !in_word {
                count += 1;
                in_word = true;
            }
        } else {
            in_word = false;
            if !character.is_whitespace() {
                count += 1;
            }
        }
    }
    count.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic::schema::MessagesRequest;
    use crate::providers::grok::translate::request::translate_request;
    use base64::Engine as _;
    use serde_json::json;

    fn png_base64(width: u32, height: u32) -> String {
        let mut header = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        header.extend_from_slice(&width.to_be_bytes());
        header.extend_from_slice(&height.to_be_bytes());
        base64::engine::general_purpose::STANDARD.encode(header)
    }

    fn translated_request(value: serde_json::Value) -> GrokResponsesRequest {
        let request: MessagesRequest = serde_json::from_value(value).unwrap();
        translate_request(&request, "grok-4.5".into()).unwrap()
    }

    #[test]
    fn count_tokens_returns_a_positive_count() {
        let request = translated_request(json!({
            "model": "grok-4.5",
            "messages": [{"role": "user", "content": "hello"}]
        }));

        assert!(count_tokens(&request) > 0);
    }

    #[test]
    fn count_tokens_increases_for_more_input() {
        let short = translated_request(json!({
            "model": "grok-4.5",
            "messages": [{"role": "user", "content": "hello"}]
        }));
        let long = translated_request(json!({
            "model": "grok-4.5",
            "system": "Follow all instructions carefully.",
            "messages": [{"role": "user", "content": "hello, please explain this request in detail"}],
            "tools": [{"name": "lookup", "description": "Look up a record", "input_schema": {"type": "object"}}]
        }));

        assert!(count_tokens(&long) > count_tokens(&short));
    }

    #[test]
    fn count_tokens_is_deterministic() {
        let request = translated_request(json!({
            "model": "grok-4.5",
            "messages": [{"role": "user", "content": "repeatable input"}]
        }));

        assert_eq!(count_tokens(&request), count_tokens(&request));
    }

    #[test]
    fn count_tokens_handles_images_without_counting_base64_bytes_as_text() {
        let small = translated_request(json!({
            "model": "grok-4.5",
            "messages": [{"role": "user", "content": [{
                "type":"image",
                "source":{"type":"base64","media_type":"image/png","data":"aGVsbG8="}
            }]}]
        }));
        let larger_payload = translated_request(json!({
            "model": "grok-4.5",
            "messages": [{"role": "user", "content": [{
                "type":"image",
                "source":{"type":"base64","media_type":"image/png","data":"aGVsbG8gd29ybGQ="}
            }]}]
        }));

        assert!(count_tokens(&small) >= IMAGE_OVERHEAD_TOKENS);
        assert_eq!(count_tokens(&small), count_tokens(&larger_payload));
    }

    #[test]
    fn streaming_base64_validation_enforces_the_decoded_byte_boundary() {
        let at_limit = base64::engine::general_purpose::STANDARD.encode([0_u8; 8]);
        let over_limit = base64::engine::general_purpose::STANDARD.encode([0_u8; 9]);

        assert!(validate_and_estimate_base64_image(&at_limit, 8).is_ok());
        assert_eq!(
            validate_and_estimate_base64_image(&over_limit, 8),
            Err(Base64ImageError::TooLarge)
        );
    }

    #[test]
    fn streaming_base64_validation_reads_and_validates_the_entire_payload() {
        let mut invalid_tail = base64::engine::general_purpose::STANDARD.encode([0_u8; 16 * 1024]);
        invalid_tail.pop();
        invalid_tail.push('%');

        assert_eq!(
            validate_and_estimate_base64_image(&invalid_tail, 20 * 1024),
            Err(Base64ImageError::Invalid)
        );
    }

    #[test]
    fn streaming_image_estimate_reads_png_and_jpeg_dimensions() {
        let png = png_base64(4096, 2048);
        assert_eq!(
            validate_and_estimate_base64_image(&png, 1024).unwrap(),
            8 * 4 * IMAGE_TILE_TOKENS
        );

        let jpeg = base64::engine::general_purpose::STANDARD.encode([
            0xff, 0xd8, // SOI
            0xff, 0xc0, 0x00, 0x07, 0x08, // SOF + length + precision
            0x08, 0x00, // height = 2048
            0x10, 0x00, // width = 4096
        ]);
        assert_eq!(
            validate_and_estimate_base64_image(&jpeg, 1024).unwrap(),
            8 * 4 * IMAGE_TILE_TOKENS
        );
    }

    #[test]
    fn streaming_image_validation_rejects_dimensions_below_grok_minimum() {
        for image in [
            png_base64(1, 512),
            png_base64(512, 1),
            png_base64(7, 73),
            png_base64(8, 8),
            png_base64(8, 63),
        ] {
            assert_eq!(
                validate_and_estimate_base64_image(&image, 1024),
                Err(Base64ImageError::TooSmall)
            );
        }
        for image in [png_base64(8, 64), png_base64(16, 32), png_base64(512, 8)] {
            assert!(validate_and_estimate_base64_image(&image, 1024).is_ok());
        }
    }

    #[test]
    fn jpeg_metadata_beyond_the_bounded_prefix_uses_decoded_size_fallback() {
        let mut jpeg = vec![0xff, 0xd8, 0xff, 0xe0, 0xff, 0xff];
        jpeg.resize(2 + 2 + usize::from(u16::MAX), 0);
        jpeg.extend_from_slice(&[
            0xff, 0xc0, 0x00, 0x07, 0x08, // SOF + length + precision
            0x10, 0x00, // height = 4096
            0x10, 0x00, // width = 4096
        ]);
        assert!(jpeg.len() > IMAGE_METADATA_PREFIX_BYTES);
        let encoded = base64::engine::general_purpose::STANDARD.encode(&jpeg);

        assert_eq!(
            validate_and_estimate_base64_image(&encoded, jpeg.len()).unwrap(),
            IMAGE_OVERHEAD_TOKENS
        );
    }

    #[test]
    fn translated_base64_image_reuses_validation_time_estimate() {
        let mut request = translated_request(json!({
            "model": "grok-4.5",
            "messages": [{"role": "user", "content": [{
                "type":"image",
                "source":{"type":"base64","media_type":"image/png","data":png_base64(1024, 1024)}
            }]}]
        }));
        let before = count_tokens(&request);
        let image = request
            .input
            .iter_mut()
            .find_map(|item| match item {
                GrokInputItem::Message { content, .. } => {
                    content.iter_mut().find_map(|part| match part {
                        GrokContentPart::InputImage {
                            image_url,
                            estimated_tokens,
                        } => Some((image_url, estimated_tokens)),
                        _ => None,
                    })
                }
                _ => None,
            })
            .expect("translated image");
        assert!(image.1.is_some());
        *image.0 = "data:image/png;base64,this-is-deliberately-invalid".into();

        // If counting decoded the URL again this would fall back to a different estimate.
        assert_eq!(count_tokens(&request), before);
    }

    #[test]
    fn count_tokens_scales_base64_images_by_pixel_dimensions() {
        let tiny = translated_request(json!({
            "model": "grok-4.5",
            "messages": [{"role": "user", "content": [{
                "type":"image",
                "source":{"type":"base64","media_type":"image/png","data":png_base64(16, 32)}
            }]}]
        }));
        let large = translated_request(json!({
            "model": "grok-4.5",
            "messages": [{"role": "user", "content": [{
                "type":"image",
                "source":{"type":"base64","media_type":"image/png","data":png_base64(4096, 4096)}
            }]}]
        }));

        assert_eq!(
            estimate_image_tokens(&format!("data:image/png;base64,{}", png_base64(1, 1))),
            IMAGE_OVERHEAD_TOKENS
        );
        assert!(count_tokens(&large) > count_tokens(&tiny));
    }

    #[test]
    fn image_estimate_is_bounded_for_untrusted_dimensions() {
        let encoded = png_base64(u32::MAX, u32::MAX);
        assert_eq!(
            estimate_image_tokens(&format!("data:image/png;base64,{encoded}")),
            MAX_IMAGE_ESTIMATE_TOKENS
        );
    }
}
