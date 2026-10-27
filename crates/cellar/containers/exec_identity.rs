// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A PID observed inside the container, before exec replaces the POSIX shell.
//! Docker's exec-inspect PID belongs to the daemon namespace and is never a
//! valid substitute for this handshake when signalling inside the container.
use anyhow::{ensure, Context as _, Result};
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt};

const PREFIX: &[u8] = b"cellar-exec-pid:";
const MAX_HEADER: usize = 64;

pub(super) fn command(argv: &[String]) -> Result<Vec<String>> {
    ensure!(
        argv.first()
            .is_some_and(|value| !value.is_empty() && !value.starts_with('-'))
            && argv.iter().all(|value| !value.contains('\0')),
        "exec requires a nonempty executable without a leading hyphen, and NUL-free argv"
    );
    let mut command = vec![
        "/bin/sh".into(),
        "-c".into(),
        "printf 'cellar-exec-pid:%s\\n' \"$$\"; exec \"$@\"".into(),
        "cellar-exec".into(),
    ];
    // Application values remain individual argv entries, never shell source.
    command.extend_from_slice(argv);
    Ok(command)
}

/// Read the private prelude before spawning a pump. Cancellation during admission
/// then drops the socket directly, without leaving an unregistered reader task.
pub(super) struct Identity {
    pub pid: i64,
    pub stdout: Bytes,
    pub stderr: Vec<Bytes>,
}

pub(super) async fn receive(read: &mut (impl AsyncRead + Unpin)) -> Result<Identity> {
    let mut decoder = Header::default();
    let mut stderr = Vec::new();
    let mut stderr_bytes = 0;
    loop {
        let mut header = [0; 8];
        read.read_exact(&mut header)
            .await
            .context("exec namespace identity was not received")?;
        let (stream, length) = super::frame_header(&header);
        // Bound admission independently of consumers, which do not exist yet.
        ensure!(length <= 64 * 1024, "exec identity frame exceeds limit");
        let mut payload = vec![0; length];
        read.read_exact(&mut payload).await?;
        if stream == 2 {
            stderr_bytes += payload.len();
            ensure!(
                stderr.len() < 15 && stderr_bytes <= 64 * 1024,
                "stderr before exec identity exceeds limit"
            );
            stderr.push(Bytes::from(payload));
        } else {
            ensure!(stream == 1, "invalid stream before exec identity");
            if let Some((pid, stdout)) = decoder.push(Bytes::from(payload))? {
                return Ok(Identity {
                    pid,
                    stdout,
                    stderr,
                });
            }
        }
    }
}

#[derive(Default)]
pub(super) struct Header(Vec<u8>);

impl Header {
    pub fn push(&mut self, payload: Bytes) -> Result<Option<(i64, Bytes)>> {
        let newline = payload.iter().position(|byte| *byte == b'\n');
        let length = newline.map_or(payload.len(), |index| index + 1);
        ensure!(
            self.0.len() + length <= MAX_HEADER,
            "exec identity header exceeds limit"
        );
        self.0.extend_from_slice(&payload[..length]);
        let Some(_) = newline else { return Ok(None) };
        let digits = self
            .0
            .strip_prefix(PREFIX)
            .context("invalid exec identity prefix")?;
        let digits = &digits[..digits.len() - 1];
        ensure!(
            !digits.is_empty() && digits.iter().all(u8::is_ascii_digit),
            "invalid exec namespace PID"
        );
        let pid: i64 = std::str::from_utf8(digits)?.parse()?;
        ensure!(
            (2..=i32::MAX as i64).contains(&pid),
            "invalid exec namespace PID"
        );
        Ok(Some((pid, payload.slice(length..))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragmented_identity_preserves_the_exact_application_prefix() -> Result<()> {
        let header = b"cellar-exec-pid:42\n";
        for split in 0..header.len() {
            let mut decoder = Header::default();
            assert!(decoder
                .push(Bytes::copy_from_slice(&header[..split]))?
                .is_none());
            let mut rest = header[split..].to_vec();
            rest.extend_from_slice(&[0, 255, b'\n']);
            let (pid, body) = decoder.push(rest.into())?.unwrap();
            assert_eq!(pid, 42);
            assert_eq!(body.as_ref(), &[0, 255, b'\n']);
        }
        let (_, body) = Header::default().push(Bytes::from_static(header))?.unwrap();
        assert!(
            body.is_empty(),
            "identity-only frames must not publish a guest EOF"
        );
        Ok(())
    }

    #[test]
    fn malformed_or_unbounded_identity_never_becomes_a_daemon_pid() {
        for value in [
            "other:42\n",
            "cellar-exec-pid:0\n",
            "cellar-exec-pid:1\n",
            "cellar-exec-pid:-2\n",
            "cellar-exec-pid:2147483648\n",
            "cellar-exec-pid:\n",
        ] {
            assert!(Header::default()
                .push(Bytes::copy_from_slice(value.as_bytes()))
                .is_err());
        }
        assert!(Header::default()
            .push(Bytes::from(vec![b'x'; MAX_HEADER + 1]))
            .is_err());
    }

    fn frame(stream: u8, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![stream, 0, 0, 0];
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    #[tokio::test]
    async fn admission_retains_bounded_stderr_and_binary_stdout() -> Result<()> {
        let mut wire = frame(2, b"diagnostic");
        wire.extend(frame(1, b"cellar-exec-pid:42\n\0\xff"));
        let identity = receive(&mut wire.as_slice()).await?;
        assert_eq!(identity.pid, 42);
        assert_eq!(identity.stdout.as_ref(), &[0, 255]);
        assert_eq!(identity.stderr, [Bytes::from_static(b"diagnostic")]);
        Ok(())
    }

    #[tokio::test]
    async fn admission_refuses_unbounded_stderr_instead_of_waiting_for_a_consumer() {
        let wire = frame(2, &vec![0; 64 * 1024 + 1]);
        assert!(receive(&mut wire.as_slice()).await.is_err());
        let wire = frame(2, b"x").repeat(16);
        assert!(receive(&mut wire.as_slice()).await.is_err());
    }

    #[test]
    fn argv_is_not_interpolated_into_shell_source() {
        let argv = vec!["tool".into(), "$(touch /bad); 'quoted'\n".into()];
        let wrapped = command(&argv).unwrap();
        assert_eq!(&wrapped[4..], argv);
        assert!(!wrapped[2].contains("/bad"));
        for argv in [
            vec![],
            vec!["".into()],
            vec!["-tool".into()],
            vec!["a\0b".into()],
        ] {
            assert!(command(&argv).is_err());
        }
    }
}
