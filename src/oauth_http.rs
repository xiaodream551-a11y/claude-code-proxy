use std::io::Read;

use futures_util::StreamExt;
use serde::de::DeserializeOwned;

pub(crate) const MAX_OAUTH_JSON_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_OAUTH_ERROR_BYTES: usize = 64 * 1024;

pub(crate) fn is_loopback_url(raw: &str) -> bool {
    let Ok(url) = url::Url::parse(raw) else {
        return false;
    };
    match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

fn body_capacity(content_length: Option<u64>, limit: usize) -> usize {
    content_length.unwrap_or_default().min(limit as u64) as usize
}

fn reject_oversized_content_length(
    content_length: Option<u64>,
    limit: usize,
    label: &str,
) -> anyhow::Result<()> {
    if content_length.is_some_and(|length| length > limit as u64) {
        anyhow::bail!("{label} exceeds the {limit}-byte size limit");
    }
    Ok(())
}

async fn read_limited_async(
    response: reqwest::Response,
    limit: usize,
    label: &str,
) -> anyhow::Result<Vec<u8>> {
    reject_oversized_content_length(response.content_length(), limit, label)?;
    let mut body = Vec::with_capacity(body_capacity(response.content_length(), limit));
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if chunk.len() > limit.saturating_sub(body.len()) {
            anyhow::bail!("{label} exceeds the {limit}-byte size limit");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn read_limited_blocking(
    mut response: reqwest::blocking::Response,
    limit: usize,
    label: &str,
) -> anyhow::Result<Vec<u8>> {
    reject_oversized_content_length(response.content_length(), limit, label)?;
    let mut body = Vec::with_capacity(body_capacity(response.content_length(), limit));
    response
        .by_ref()
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut body)?;
    if body.len() > limit {
        anyhow::bail!("{label} exceeds the {limit}-byte size limit");
    }
    Ok(body)
}

pub(crate) async fn read_json_async<T: DeserializeOwned>(
    response: reqwest::Response,
    limit: usize,
    label: &str,
) -> anyhow::Result<T> {
    let body = read_limited_async(response, limit, label).await?;
    serde_json::from_slice(&body).map_err(Into::into)
}

pub(crate) async fn read_text_async(
    response: reqwest::Response,
    limit: usize,
    label: &str,
) -> anyhow::Result<String> {
    let body = read_limited_async(response, limit, label).await?;
    Ok(String::from_utf8_lossy(&body).into_owned())
}

pub(crate) fn read_json_blocking<T: DeserializeOwned>(
    response: reqwest::blocking::Response,
    limit: usize,
    label: &str,
) -> anyhow::Result<T> {
    let body = read_limited_blocking(response, limit, label)?;
    serde_json::from_slice(&body).map_err(Into::into)
}

pub(crate) fn read_text_blocking(
    response: reqwest::blocking::Response,
    limit: usize,
    label: &str,
) -> anyhow::Result<String> {
    let body = read_limited_blocking(response, limit, label)?;
    Ok(String::from_utf8_lossy(&body).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::net::TcpListener;

    fn spawn_chunked_response(chunks: Vec<Vec<u8>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            for chunk in chunks {
                if write!(stream, "{:x}\r\n", chunk.len()).is_err()
                    || stream.write_all(&chunk).is_err()
                    || stream.write_all(b"\r\n").is_err()
                {
                    return;
                }
            }
            let _ = stream.write_all(b"0\r\n\r\n");
        });
        url
    }

    fn spawn_fixed_response(body: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request);
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(headers.as_bytes()).unwrap();
            let _ = stream.write_all(&body);
        });
        url
    }

    #[test]
    fn content_length_limit_accepts_boundary_and_rejects_next_byte() {
        reject_oversized_content_length(Some(64), 64, "OAuth body").unwrap();
        let error = reject_oversized_content_length(Some(65), 64, "OAuth body").unwrap_err();
        assert!(error.to_string().contains("64-byte size limit"));
    }

    #[test]
    fn loopback_url_detection_covers_localhost_and_ip_literals() {
        assert!(is_loopback_url("http://localhost:18765/v1"));
        assert!(is_loopback_url("http://127.0.0.1:18765/v1"));
        assert!(is_loopback_url("http://[::1]:18765/v1"));
        assert!(!is_loopback_url("https://api.openai.com/v1"));
        assert!(!is_loopback_url("not a URL"));
    }

    #[test]
    fn blocking_reader_rejects_chunked_body_past_limit() {
        let url = spawn_chunked_response(vec![vec![b'a'; 32], vec![b'b'; 33]]);
        let response = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .unwrap()
            .get(url)
            .send()
            .unwrap();
        let error = read_limited_blocking(response, 64, "OAuth body").unwrap_err();
        assert!(error.to_string().contains("64-byte size limit"));
    }

    #[test]
    fn blocking_reader_accepts_body_at_exact_limit() {
        let url = spawn_fixed_response(vec![b'a'; 64]);
        let response = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .unwrap()
            .get(url)
            .send()
            .unwrap();
        assert_eq!(
            read_limited_blocking(response, 64, "OAuth body").unwrap(),
            vec![b'a'; 64]
        );
    }

    #[test]
    fn blocking_reader_rejects_oversized_content_length() {
        let url = spawn_fixed_response(vec![b'a'; 65]);
        let response = reqwest::blocking::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .unwrap()
            .get(url)
            .send()
            .unwrap();
        let error = read_limited_blocking(response, 64, "OAuth body").unwrap_err();
        assert!(error.to_string().contains("64-byte size limit"));
    }

    #[tokio::test]
    async fn async_reader_rejects_chunked_body_past_limit() {
        let url = spawn_chunked_response(vec![vec![b'a'; 32], vec![b'b'; 33]]);
        let response = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .unwrap()
            .get(url)
            .send()
            .await
            .unwrap();
        let error = read_limited_async(response, 64, "OAuth body")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("64-byte size limit"));
    }
}
