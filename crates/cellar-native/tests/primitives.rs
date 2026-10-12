use cellar_native::compression::{transform, CompressionStream};
use cellar_native::crypto::{self, AesBlockMode, DigestAlgorithm, DigestStream, HashAlgorithm};
use cellar_native::encoding::{self, DecodeError, DecoderOptions, TextDecoder};
use serde_json::{json, Value};

fn bytes(value: &Value, field: &str) -> Vec<u8> {
    serde_json::from_value(value[field].clone()).unwrap()
}

#[test]
fn digest_vectors_and_incremental_checksums() {
    for (algorithm, expected) in [
        ("MD5", "900150983cd24fb0d6963f7d28e17f72"),
        ("SHA-1", "a9993e364706816aba3e25717850c26c9cd0d89d"),
        ("SHA-224", "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"),
        ("SHA-256", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
        ("SHA-384", "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"),
        ("SHA-512", "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"),
    ] {
        let algorithm: DigestAlgorithm = algorithm.parse().unwrap();
        assert_eq!(hex::encode(crypto::digest(algorithm, b"abc")), expected);
        let mut stream = DigestStream::new(algorithm);
        for byte in b"abc" { stream.update(&[*byte]); }
        assert_eq!(hex::encode(stream.finish()), expected);
    }
    for (algorithm, expected) in [
        ("CRC32", "cbf43926"),
        ("CRC32C", "e3069283"),
        ("CRC64NVME", "ae8b14860a799888"),
    ] {
        let mut stream = DigestStream::new(algorithm.parse().unwrap());
        for byte in b"123456789" {
            stream.update(&[*byte]);
        }
        assert_eq!(hex::encode(stream.finish()), expected);
    }
    assert!("SHA-999".parse::<DigestAlgorithm>().is_err());
}

#[test]
fn hmac_vectors_and_verification_for_all_retained_hashes() {
    let expected = "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7";
    let key = [0x0b; 20];
    assert_eq!(
        hex::encode(crypto::hmac_sign(HashAlgorithm::Sha256, &key, b"Hi There")),
        expected
    );
    for hash in [
        HashAlgorithm::Md5,
        HashAlgorithm::Sha1,
        HashAlgorithm::Sha224,
        HashAlgorithm::Sha256,
        HashAlgorithm::Sha384,
        HashAlgorithm::Sha512,
    ] {
        let signature = crypto::hmac_sign(hash, &key, b"Hi There");
        assert_eq!(signature.len(), hash.output_len());
        assert!(crypto::hmac_verify(hash, &key, &signature, b"Hi There"));
        assert!(!crypto::hmac_verify(hash, &key, &signature, b"wrong"));
        assert!(!crypto::hmac_verify(
            hash,
            &key,
            &signature[..signature.len() - 1],
            b"Hi There"
        ));
    }
}

#[test]
fn kdf_vectors_and_boundaries() {
    let output = crypto::pbkdf2(HashAlgorithm::Sha1, b"password", b"salt", 1, 20).unwrap();
    assert_eq!(
        hex::encode(output),
        "0c60c80f961f0e71f3a9b524af6012062fe037a6"
    );
    let ikm = [0x0b; 22];
    let salt = hex::decode("000102030405060708090a0b0c").unwrap();
    let info = hex::decode("f0f1f2f3f4f5f6f7f8f9").unwrap();
    let output = crypto::hkdf(HashAlgorithm::Sha256, &ikm, &salt, &info, 42).unwrap();
    assert_eq!(
        hex::encode(output),
        "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"
    );
    for hash in [
        HashAlgorithm::Md5,
        HashAlgorithm::Sha1,
        HashAlgorithm::Sha224,
        HashAlgorithm::Sha256,
        HashAlgorithm::Sha384,
        HashAlgorithm::Sha512,
    ] {
        assert_eq!(crypto::pbkdf2(hash, b"p", b"s", 1, 17).unwrap().len(), 17);
        let maximum = 255 * hash.output_len();
        assert_eq!(
            crypto::hkdf(hash, &ikm, &salt, &info, maximum)
                .unwrap()
                .len(),
            maximum
        );
        assert!(crypto::hkdf(hash, &ikm, &salt, &info, maximum + 1).is_err());
        assert!(crypto::hkdf(hash, &ikm, &salt, &info, usize::MAX).is_err());
        assert!(crypto::pbkdf2(hash, b"p", b"s", 0, 1).is_err());
    }
}

#[test]
fn aes_gcm_vector_authentication_and_retained_sizes() {
    let output = crypto::aes_gcm(&[0; 16], &[0; 12], &[], &[0; 16], 16, true).unwrap();
    assert_eq!(
        hex::encode(&output),
        "0388dace60b6a392f328c2b971b2fe78ab6e47d42cec13bdf53a67b21257bddf"
    );
    for key_length in [16, 32] {
        for nonce_length in [12, 16] {
            for tag in 12..=16 {
                let key = vec![1; key_length];
                let nonce = vec![2; nonce_length];
                let encrypted =
                    crypto::aes_gcm(&key, &nonce, b"aad", b"native cell", tag, true).unwrap();
                assert_eq!(
                    crypto::aes_gcm(&key, &nonce, b"aad", &encrypted, tag, false).unwrap(),
                    b"native cell"
                );
                assert!(
                    crypto::aes_gcm(&key, &nonce, b"changed", &encrypted, tag, false).is_none()
                );
            }
        }
    }
    assert!(crypto::aes_gcm(&[0; 15], &[0; 12], &[], &[], 16, true).is_none());
    assert!(crypto::aes_gcm(&[0; 16], &[], &[], &[], 16, true).is_none());
    assert!(crypto::aes_gcm(&[0; 16], &[0; 12], &[], &[], 11, true).is_none());
}

#[test]
fn aes_ctr_vector_and_cbc_padding() {
    let key = hex::decode("2b7e151628aed2a6abf7158809cf4f3c").unwrap();
    let counter = hex::decode("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff").unwrap();
    let plaintext = hex::decode("6bc1bee22e409f96e93d7e117393172a").unwrap();
    let encrypted =
        crypto::aes_block_mode(AesBlockMode::Ctr, &key, &counter, &plaintext, true).unwrap();
    assert_eq!(hex::encode(&encrypted), "874d6191b620e3261bef6864990db6ce");
    assert_eq!(hex::encode(&counter), "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff");
    for mode in [AesBlockMode::Cbc, AesBlockMode::Ctr] {
        for key in [vec![3; 16], vec![3; 32]] {
            for plaintext in [b"".as_slice(), b"short", &[4; 32]] {
                let encrypted =
                    crypto::aes_block_mode(mode, &key, &[2; 16], plaintext, true).unwrap();
                assert_eq!(
                    crypto::aes_block_mode(mode, &key, &[2; 16], &encrypted, false).unwrap(),
                    plaintext
                );
            }
        }
        assert!(crypto::aes_block_mode(mode, &[0; 15], &[0; 16], b"", true).is_err());
        assert!(crypto::aes_block_mode(mode, &[0; 16], &[0; 15], b"", true).is_err());
    }
    assert!(crypto::aes_block_mode(AesBlockMode::Cbc, &[0; 16], &[0; 16], &[0; 1], false).is_err());
}

#[test]
fn random_quota_and_constant_time_equality() {
    let first = crypto::random_uuid().unwrap();
    let second = crypto::random_uuid().unwrap();
    assert_ne!(first, second);
    assert_eq!(first.len(), 36);
    assert_eq!(&first[14..15], "4");
    assert!(matches!(first.as_bytes()[19], b'8' | b'9' | b'a' | b'b'));
    assert!(first.bytes().enumerate().all(|(index, byte)| {
        if [8, 13, 18, 23].contains(&index) {
            byte == b'-'
        } else {
            byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
        }
    }));
    let mut bytes = vec![0; 65_536];
    crypto::random_values(&mut bytes).unwrap();
    assert!(bytes.iter().any(|byte| *byte != 0));
    let mut oversized = vec![0x5a; 65_537];
    assert!(crypto::random_values(&mut oversized).is_err());
    assert!(oversized.iter().all(|byte| *byte == 0x5a));
    assert!(crypto::timing_safe_equal(b"cell", b"cell"));
    assert!(!crypto::timing_safe_equal(b"cell", b"calls"));
    assert!(!crypto::timing_safe_equal(b"cell", b"call"));
}

fn key_pair(kind: &str, curve: Option<&str>) -> Value {
    crypto::operation(
        "asym-key-generate",
        &json!({
            "type": kind, "namedCurve": curve, "modulusLength": 1024,
        }),
    )
    .unwrap()
}

fn assert_key_formats(pair: &Value) {
    for (visibility, field) in [("private", "privateDer"), ("public", "publicDer")] {
        let der = &pair[field];
        let imported = crypto::operation(
            "asym-key-import",
            &json!({
                "key": der, "format": "der", "visibility": visibility,
                "type": if visibility == "private" {"pkcs8"} else {"spki"},
            }),
        )
        .unwrap();
        assert_eq!(imported["der"], *der);
        let pem = crypto::operation(
            "asym-key-reencode",
            &json!({
                "der": der, "visibility": visibility, "format": "pem",
            }),
        )
        .unwrap();
        let imported = crypto::operation("asym-key-import", &json!({
            "key": pem["pem"].as_str().unwrap().as_bytes(), "format": "pem", "visibility": visibility,
        })).unwrap();
        assert_eq!(imported["der"], *der);
        let exported = crypto::operation(
            "asym-key-export",
            &json!({
                "der": der, "visibility": visibility,
            }),
        )
        .unwrap();
        let imported = crypto::operation(
            "asym-key-import",
            &json!({
                "key": exported["jwk"], "format": "jwk", "visibility": visibility,
            }),
        )
        .unwrap();
        assert_eq!(imported["der"], *der);
    }
    let public = crypto::operation(
        "asym-key-public-from-private",
        &json!({"der": pair["privateDer"]}),
    )
    .unwrap();
    assert_eq!(public["der"], pair["publicDer"]);
}

fn assert_signature(pair: &Value, sign: &str, verify: &str, hash: &str) {
    let signed = crypto::operation(
        sign,
        &json!({
            "key": pair["privateDer"], "data": b"native cell", "hash": hash, "saltLength": 32,
        }),
    )
    .unwrap();
    for (data, expected) in [(b"native cell".as_slice(), true), (b"tampered", false)] {
        let verified = crypto::operation(
            verify,
            &json!({
                "key": pair["publicDer"], "signature": signed["bytes"], "data": data, "hash": hash,
            }),
        )
        .unwrap();
        assert_eq!(verified["ok"], expected);
    }
}

#[test]
fn ed25519_formats_signatures_and_raw_public_keys() {
    let pair = key_pair("ed25519", None);
    assert_key_formats(&pair);
    assert_signature(&pair, "ed25519-sign", "ed25519-verify", "SHA-256");
    let raw = crypto::operation("asym-key-raw-public", &json!({"der": pair["publicDer"]})).unwrap();
    let imported = crypto::operation(
        "asym-key-import",
        &json!({
            "key": raw["bytes"], "format": "raw", "type": "Ed25519", "visibility": "public",
        }),
    )
    .unwrap();
    assert_eq!(imported["der"], pair["publicDer"]);
}

#[test]
fn all_nist_curves_formats_signatures_and_ecdh() {
    for (curve, hash, secret_len) in [
        ("P-256", "SHA-256", 32),
        ("P-384", "SHA-384", 48),
        ("P-521", "SHA-512", 66),
    ] {
        let first = key_pair("ec", Some(curve));
        let second = key_pair("ec", Some(curve));
        assert_key_formats(&first);
        assert_signature(&first, "ec-sign-der", "ec-verify-der", hash);
        if curve == "P-256" {
            assert_signature(&first, "p256-sign", "p256-verify", hash);
        }
        let left = crypto::operation(
            "ecdh-derive",
            &json!({"private": first["privateDer"], "public": second["publicDer"]}),
        )
        .unwrap();
        let right = crypto::operation(
            "ecdh-derive",
            &json!({"private": second["privateDer"], "public": first["publicDer"]}),
        )
        .unwrap();
        assert_eq!(left, right);
        assert_eq!(bytes(&left, "bytes").len(), secret_len);
    }
    let first = key_pair("ec", Some("P-256"));
    let second = key_pair("ec", Some("P-384"));
    assert!(crypto::operation(
        "ecdh-derive",
        &json!({"private": first["privateDer"], "public": second["publicDer"]})
    )
    .is_err());
}

#[test]
fn x25519_formats_agreement_and_low_order_rejection() {
    let first = key_pair("x25519", None);
    let second = key_pair("x25519", None);
    assert_key_formats(&first);
    let raw =
        crypto::operation("asym-key-raw-public", &json!({"der": second["publicDer"]})).unwrap();
    let imported = crypto::operation(
        "asym-key-import",
        &json!({
            "key": raw["bytes"], "format": "raw", "type": "X25519", "visibility": "public",
        }),
    )
    .unwrap();
    assert_eq!(imported["der"], second["publicDer"]);
    let left = crypto::operation(
        "x25519-derive",
        &json!({"private": first["privateDer"], "public": second["publicDer"]}),
    )
    .unwrap();
    let right = crypto::operation(
        "x25519-derive",
        &json!({"private": second["privateDer"], "public": first["publicDer"]}),
    )
    .unwrap();
    assert_eq!(left, right);
    let low_order = crypto::operation(
        "asym-key-import",
        &json!({
            "key": ([0; 32].as_slice()), "format": "raw", "type": "X25519", "visibility": "public",
        }),
    )
    .unwrap();
    assert!(crypto::operation(
        "x25519-derive",
        &json!({"private": first["privateDer"], "public": low_order["der"]})
    )
    .is_err());
}

#[test]
fn rsa_formats_signatures_blinded_oaep_and_encrypted_pkcs8() {
    let pair = key_pair("rsa", None);
    assert_key_formats(&pair);
    assert_signature(&pair, "rsa-pkcs1-sign", "rsa-pkcs1-verify", "SHA-256");
    assert_signature(&pair, "rsa-pss-sign", "rsa-pss-verify", "SHA-256");
    let encrypted = crypto::operation(
        "rsa-oaep-encrypt",
        &json!({
            "key": pair["publicDer"], "data": b"cell", "label": b"context",
        }),
    )
    .unwrap();
    let decrypted = crypto::operation(
        "rsa-oaep-decrypt",
        &json!({
            "key": pair["privateDer"], "data": encrypted["bytes"], "label": b"context",
        }),
    )
    .unwrap();
    assert_eq!(bytes(&decrypted, "bytes"), b"cell");
    assert!(crypto::operation(
        "rsa-oaep-decrypt",
        &json!({
            "key": pair["privateDer"], "data": encrypted["bytes"], "label": b"wrong",
        })
    )
    .is_err());
    let encoded = crypto::operation("asym-key-reencode", &json!({
        "der": pair["privateDer"], "visibility": "private", "passphrase": b"password", "cipher": "aes-256-cbc",
    })).unwrap();
    let imported = crypto::operation("asym-key-import", &json!({
        "key": encoded["der"], "format": "der", "visibility": "private", "passphrase": b"password",
    })).unwrap();
    assert_eq!(imported["der"], pair["privateDer"]);
    assert!(crypto::operation(
        "asym-key-import",
        &json!({
            "key": encoded["der"], "format": "der", "visibility": "private", "passphrase": b"wrong",
        })
    )
    .is_err());
}

#[test]
fn crypto_rejects_malformed_operations_and_key_bytes() {
    for (operation, args) in [
        ("unknown", Value::Null),
        ("ed25519-sign", json!({"key": [1,2,3], "data": []})),
        (
            "asym-key-import",
            json!({"format": "jwk", "visibility": "private", "key": {"kty": "OKP", "crv": "Ed25519"}}),
        ),
        ("asym-key-generate", json!({"type": "unsupported"})),
    ] {
        assert!(crypto::operation(operation, &args).is_err());
    }
}

#[test]
fn compression_one_shot_and_incremental_round_trips() {
    let payload = (0..100_000).map(|n| (n % 251) as u8).collect::<Vec<_>>();
    for (format, encode, decode) in [
        ("gzip", "gzip", "gunzip"),
        ("deflate", "deflate", "inflate"),
        ("deflate-raw", "deflateRaw", "inflateRaw"),
    ] {
        let encoded = transform(encode, &payload).unwrap();
        assert_eq!(transform(decode, &encoded).unwrap(), payload);
        let mut encoder = CompressionStream::new(format, false).unwrap();
        let mut encoded = Vec::new();
        for chunk in payload.chunks(137) {
            encoded.extend(encoder.push(chunk).unwrap());
        }
        encoded.extend(encoder.finish().unwrap());
        let mut decoder = CompressionStream::new(format, true).unwrap();
        let mut decoded = Vec::new();
        for chunk in encoded.chunks(7) {
            decoded.extend(decoder.push(chunk).unwrap());
        }
        decoded.extend(decoder.finish().unwrap());
        assert_eq!(decoded, payload);
        assert_eq!(
            transform(decode, &transform(encode, b"").unwrap()).unwrap(),
            b""
        );
    }
}

#[test]
fn compression_checks_truncation_trailers_and_cancellation() {
    let encoded = transform("gzip", b"cell").unwrap();
    let mut decoder = CompressionStream::new("gzip", true).unwrap();
    decoder.push(&encoded[..encoded.len() - 1]).unwrap();
    assert!(decoder.finish().is_err());
    let mut corrupted = encoded.clone();
    let position = corrupted.len() - 8;
    corrupted[position] ^= 1;
    let mut decoder = CompressionStream::new("gzip", true).unwrap();
    // flate2 can consume the trailer before reporting its checksum failure
    // on the next write or finish. No successful stream can emerge from it.
    let _ = decoder.push(&corrupted);
    assert!(decoder.push(b"x").is_err());
    assert!(decoder.push(b"").is_err());
    let mut garbage = encoded;
    garbage.extend_from_slice(b"garbage");
    let mut decoder = CompressionStream::new("gzip", true).unwrap();
    assert!(decoder.push(&garbage).is_err());
    let mut encoder = CompressionStream::new("gzip", false).unwrap();
    encoder.cancel();
    assert!(encoder.push(b"cell").is_err());
    assert!(encoder.finish().is_err());
    assert!(CompressionStream::new("unsupported", false).is_none());
}

#[test]
fn text_decoder_streaming_labels_bom_and_reuse() {
    let mut decoder = TextDecoder::new("utf8", DecoderOptions::default()).unwrap();
    assert_eq!(decoder.encoding(), "utf-8");
    assert_eq!(decoder.decode(&[0xef], true).unwrap(), "");
    assert_eq!(decoder.decode(&[0xbb, 0xbf, 0xf0, 0x9f], true).unwrap(), "");
    assert_eq!(decoder.decode(&[0x99, 0x82], false).unwrap(), "🙂");
    assert_eq!(decoder.decode(b"again", false).unwrap(), "again");
    let mut decoder = TextDecoder::new(
        "utf-8",
        DecoderOptions {
            ignore_bom: true,
            fatal: false,
        },
    )
    .unwrap();
    assert_eq!(
        decoder.decode(&[0xef, 0xbb, 0xbf], false).unwrap(),
        "\u{feff}"
    );
    let mut decoder = TextDecoder::new("shift_jis", DecoderOptions::default()).unwrap();
    assert_eq!(decoder.decode(&[0x82], true).unwrap(), "");
    assert_eq!(decoder.decode(b"", true).unwrap(), "");
    assert_eq!(decoder.decode(&[0xa0], false).unwrap(), "あ");
    assert_eq!(
        encoding::canonical_label(" latin1 ").unwrap(),
        "windows-1252"
    );
    assert!(TextDecoder::new("replacement", DecoderOptions::default()).is_err());
}

#[test]
fn text_decoder_fatal_errors_reset_state_and_utf16_is_lossy() {
    let mut decoder = TextDecoder::new(
        "utf8",
        DecoderOptions {
            fatal: true,
            ignore_bom: false,
        },
    )
    .unwrap();
    assert_eq!(
        decoder.decode(&[0xff], false).unwrap_err(),
        DecodeError::InvalidData
    );
    assert_eq!(decoder.decode(b"ok", false).unwrap(), "ok");
    assert_eq!(decoder.decode(&[0xe2], true).unwrap(), "");
    assert_eq!(
        decoder.decode(b"", false).unwrap_err(),
        DecodeError::InvalidData
    );
    assert_eq!(decoder.decode(b"ok", false).unwrap(), "ok");
    let mut decoder = TextDecoder::new("utf-16le", DecoderOptions::default()).unwrap();
    assert_eq!(decoder.decode(&[0x00, 0xd8, 0x01], false).unwrap(), "�");
}

#[test]
fn text_encoder_does_not_split_scalars_and_counts_utf16_units() {
    let source = [0x61, 0xd83d, 0xde42, 0xd800, 0x62];
    assert_eq!(encoding::encode_utf16(&source), "a🙂�b".as_bytes());
    let mut destination = [0xcc; 4];
    let result = encoding::encode_into_utf16(&source, &mut destination);
    assert_eq!((result.read, result.written), (1, 1));
    assert_eq!(destination, [b'a', 0xcc, 0xcc, 0xcc]);
    let mut destination = [0; 5];
    let result = encoding::encode_into_utf16(&source, &mut destination);
    assert_eq!((result.read, result.written), (3, 5));
    assert_eq!(&destination, "a🙂".as_bytes());
}

#[test]
fn native_resources_can_cross_executor_threads() {
    fn assert_send<T: Send>() {}
    assert_send::<CompressionStream>();
    assert_send::<DigestStream>();
    assert_send::<TextDecoder>();
    let mut decoder = TextDecoder::new("utf8", DecoderOptions::default()).unwrap();
    decoder.decode(&[0xf0, 0x9f], true).unwrap();
    let decoded = std::thread::spawn(move || decoder.decode(&[0x99, 0x82], false).unwrap())
        .join()
        .unwrap();
    assert_eq!(decoded, "🙂");
}
