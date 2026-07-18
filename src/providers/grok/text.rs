/// Return the longest UTF-8 prefix whose encoded length does not exceed `max_bytes`.
///
/// The boolean reports whether any bytes were omitted. Keeping this provider-local avoids
/// slightly different boundary loops in Grok protocol errors and diagnostics.
pub(super) fn truncate_utf8(value: &str, max_bytes: usize) -> (&str, bool) {
    if value.len() <= max_bytes {
        return (value, false);
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (&value[..end], true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_never_splits_utf8() {
        let value = "abc😊def";
        for limit in 0..value.len() {
            let (prefix, truncated) = truncate_utf8(value, limit);
            assert!(prefix.len() <= limit);
            assert!(truncated);
            assert!(value.starts_with(prefix));
        }
        assert_eq!(truncate_utf8(value, value.len()), (value, false));
    }
}
