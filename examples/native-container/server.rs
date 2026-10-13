//! Dependency-free Rust HTTP peer for the cell-owned container example.

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

fn json_string(value: &str) -> String {
    let mut result = String::from("\"");
    for character in value.chars() {
        match character {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            c if c.is_control() => result.push_str(&format!("\\u{:04x}", c as u32)),
            c => result.push(c),
        }
    }
    result.push('"');
    result
}

fn serve(mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = Vec::new();
    // Finite header budget; this teaching peer handles one request per connection.
    let mut total = 0;
    let mut path = String::new();
    let mut method = String::new();
    loop {
        let byte = reader.fill_buf()?.first().copied();
        let Some(byte) = byte else {
            return Ok(());
        };
        reader.consume(1);
        total += 1;
        if total > 8192 {
            return Ok(());
        }
        line.push(byte);
        if line.ends_with(b"\r\n") {
            if path.is_empty() {
                let head = String::from_utf8_lossy(&line);
                let mut parts = head.split_whitespace();
                method = parts.next().unwrap_or_default().into();
                path = parts.next().unwrap_or("/").into();
            } else if line == b"\r\n" {
                break;
            }
            line.clear();
        }
    }
    let (status, body) = if method == "GET" {
        (
            "200 OK",
            format!(
                "{{\"cell\":{},\"path\":{}}}",
                json_string(&std::env::var("CELLAR_CELL_ID").unwrap_or_default()),
                json_string(&path)
            ),
        )
    } else {
        ("405 Method Not Allowed", "{\"error\":\"use GET\"}".into())
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn main() -> std::io::Result<()> {
    if std::env::args().any(|arg| arg == "--tool") {
        return std::io::stdout().write_all(b"{\"tool\":\"native-rust\",\"ok\":true}\n");
    }
    for stream in TcpListener::bind("0.0.0.0:8080")?.incoming() {
        let _ = serve(stream?);
    }
    Ok(())
}
