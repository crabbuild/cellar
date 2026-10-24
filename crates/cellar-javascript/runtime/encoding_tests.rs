use crate::{EventKind, JavaScriptRuntime, Options, Source};
use anyhow::Result;

#[test]
fn native_encoding_exists_during_module_initialization_and_preserves_state() -> Result<()> {
    let code = r#"
      const assert = value => { if (!value) throw Error('Encoding assertion failed'); };
      const encoder = new TextEncoder();
      assert(encoder.encoding === 'utf-8');
      assert(String(encoder.encode('a🙂\ud800b')) === '97,240,159,153,130,239,191,189,98');
      const destination = new Uint8Array(5);
      const result = encoder.encodeInto('a🙂b', destination);
      assert(result.read === 3 && result.written === 5);
      const decoder = new TextDecoder('utf8', {fatal:true});
      assert(decoder.decode(Uint8Array.of(0xef), {stream:true}) === '');
      assert(decoder.decode(Uint8Array.of(0xbb,0xbf,0xf0,0x9f), {stream:true}) === '');
      assert(decoder.decode(Uint8Array.of(0x99,0x82)) === '🙂');
      assert(decoder.decode(Uint8Array.of(111,107)) === 'ok');
      let invalid = false;
      try {decoder.decode(Uint8Array.of(255))} catch {invalid=true}
      assert(invalid && decoder.decode(Uint8Array.of(111,107)) === 'ok');
      assert(new TextDecoder('utf-8', {ignoreBOM:true}).decode(Uint8Array.of(239,187,191)) === '\ufeff');
      const legacy = new TextDecoder('shift_jis');
      assert(legacy.decode(Uint8Array.of(0x82), {stream:true}) === '');
      assert(legacy.decode(new Uint8Array(), {stream:true}) === '');
      assert(legacy.decode(Uint8Array.of(0xa0)) === 'あ');
      assert(new TextDecoder(' latin1 ').encoding === 'windows-1252');
      let unknown = false;
      try { new TextDecoder('replacement') } catch {unknown=true}
      assert(unknown);
      var CellarApp = {async fetch(){ return null; }};
    "#;
    JavaScriptRuntime::run(
        Options::default(),
        Source::new(code, "encoding-test.js").events([EventKind::Fetch]),
        |_| Ok(()),
    )
}

#[test]
fn decoder_overload_refuses_without_breaking_live_decoder_state() -> Result<()> {
    let code = r#"
      const decoder = new TextDecoder('utf8', {fatal:true});
      decoder.decode(Uint8Array.of(0xf0,0x9f), {stream:true});
      let refused = false;
      try {new TextDecoder()} catch (error) {refused = error instanceof RangeError;}
      if (!refused || decoder.decode(Uint8Array.of(0x99,0x82)) !== '🙂') throw Error('Decoder capacity failure');
      var CellarApp = {async fetch(){ return null; }};
    "#;
    JavaScriptRuntime::run(
        Options {
            max_text_decoders: 1,
            ..Default::default()
        },
        Source::new(code, "decoder-capacity.js").events([EventKind::Fetch]),
        |_| Ok(()),
    )
}
