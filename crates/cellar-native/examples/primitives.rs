// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use cellar_native::compression::{transform, CompressionStream};
use cellar_native::crypto::{DigestStream, HashAlgorithm};
use cellar_native::encoding::{DecoderOptions, TextDecoder};
use cellar_native::web::{
    Blob, BlobOptions, BlobPart, File, FormData, FormValue, UrlPattern, UrlPatternInit,
    UrlPatternOptions,
};
use std::io::Write as _;

fn main() -> anyhow::Result<()> {
    let source = "A native cell can stream text, compress bytes, and checksum uploads.";
    let mut compressor = CompressionStream::new("gzip", false).unwrap();
    let mut digest = DigestStream::new(HashAlgorithm::Sha256.into());
    let mut compressed = Vec::new();
    for chunk in source.as_bytes().chunks(7) {
        digest.update(chunk);
        compressed.extend(compressor.push(chunk)?);
    }
    compressed.extend(compressor.finish()?);
    let decoded = transform("gunzip", &compressed)?;
    let mut decoder = TextDecoder::new("utf-8", DecoderOptions::default())?;
    assert_eq!(decoder.decode(&decoded, false)?, source);
    let route = UrlPattern::new(
        UrlPatternInit {
            protocol: Some("https".into()),
            hostname: Some("cells.example".into()),
            pathname: Some("/cells/:cell/uploads/:object".into()),
            ..Default::default()
        },
        None,
        UrlPatternOptions::default(),
    )?;
    let matched = route
        .exec("https://cells.example/cells/photos/uploads/first", None)?
        .expect("upload route");
    let cell = matched.pathname.groups["cell"]
        .as_deref()
        .expect("cell capture");
    let form = FormData::new();
    form.append("cell", FormValue::text(cell));
    form.append_file(
        "upload",
        &File::from_blob(
            Blob::new(
                [BlobPart::Bytes(&compressed)],
                BlobOptions::new("application/gzip"),
            )?,
            "upload.gz",
            Some(0.0),
        ),
        None,
    );
    let multipart = form.encode()?;
    let parsed = FormData::parse(&multipart.bytes, &multipart.content_type)?;
    let Some(FormValue::File(upload)) = parsed.get("upload") else {
        anyhow::bail!("expected uploaded file")
    };
    assert_eq!(transform("gunzip", &upload.bytes())?, source.as_bytes());
    writeln!(
        std::io::stdout().lock(),
        "Native upload for cell {cell}: {} bytes, SHA-256 {:02x?}",
        decoded.len(),
        digest.finish()
    )?;
    Ok(())
}
