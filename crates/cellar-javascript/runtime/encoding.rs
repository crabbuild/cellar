//! Text Encoding globals installed before application descriptors execute.
//! Native decoder state follows a weak JS function owner and is also dropped
//! with the isolate. No decoder contains SDK authority or crosses an await.
use anyhow::{Context as _, Result};
use cellar::sdk::encoding::{self, DecoderOptions, TextDecoder};
use std::{cell::RefCell, collections::HashMap, rc::Rc};

struct Decoder {
    value: TextDecoder,
    _owner: v8::Weak<v8::Function>,
}

struct Registry {
    maximum: usize,
    capacity: usize,
    next: u64,
    decoders: HashMap<u64, Decoder>,
}

type Shared = Rc<RefCell<Registry>>;

fn refuse(scope: &v8::PinScope, message: &str, range: bool) {
    if let Some(message) = v8::String::new(scope, message) {
        let error = if range {
            v8::Exception::range_error(scope, message)
        } else {
            v8::Exception::type_error(scope, message)
        };
        scope.throw_exception(error);
    }
}

fn encode(scope: &mut v8::PinScope, args: v8::FunctionCallbackArguments, mut rv: v8::ReturnValue) {
    let Some(registry) = scope.get_slot::<Shared>().cloned() else {
        return;
    };
    let maximum = registry.borrow().maximum;
    let Ok(source) = v8::Local::<v8::String>::try_from(args.get(0)) else {
        return refuse(scope, "Invalid text input", false);
    };
    if source.length() > maximum {
        return refuse(scope, "Text value exceeds limit", true);
    }
    let mut units = vec![0; source.length()];
    source.write_v2(scope, 0, &mut units, v8::WriteFlags::empty());
    let (bytes, read) = if args.get(1).is_undefined() {
        (encoding::encode_utf16(&units), units.len())
    } else {
        let Some(length) = args.get(1).uint32_value(scope) else {
            return refuse(scope, "Invalid text destination", false);
        };
        let mut bytes = vec![0; (length as usize).min(maximum)];
        let result = encoding::encode_into_utf16(&units, &mut bytes);
        bytes.truncate(result.written);
        (bytes, result.read)
    };
    if bytes.len() > maximum {
        return refuse(scope, "Text value exceeds limit", true);
    }
    let Ok(bytes) = super::binary::value(scope, Some(bytes)) else {
        return refuse(scope, "Text allocation failed", true);
    };
    let read = v8::Number::new(scope, read as f64);
    rv.set(v8::Array::new_with_elements(scope, &[bytes, read.into()]).into());
}

fn decode(scope: &mut v8::PinScope, args: v8::FunctionCallbackArguments, mut rv: v8::ReturnValue) {
    let Some(registry) = scope.get_slot::<Shared>().cloned() else {
        return;
    };
    let maximum = registry.borrow().maximum;
    let Some(id) = args.data().number_value(scope) else {
        return refuse(scope, "Invalid text decoder", false);
    };
    let Ok(Some(bytes)) = super::binary::copy(scope, args.get(0), maximum) else {
        return refuse(scope, "Invalid encoded input", false);
    };
    // Release the borrow before allocating a V8 string: GC may finalize another
    // decoder and remove it from this same registry.
    let result = {
        let mut registry = registry.borrow_mut();
        registry
            .decoders
            .get_mut(&(id as u64))
            .map(|decoder| decoder.value.decode(&bytes, args.get(1).is_true()))
    };
    let Some(result) = result else {
        return refuse(scope, "Expired text decoder", false);
    };
    let Ok(text) = result else {
        return refuse(scope, "Invalid encoded input", false);
    };
    let Some(text) = v8::String::new(scope, &text) else {
        return refuse(scope, "Text allocation failed", true);
    };
    rv.set(text.into());
}

fn create_decoder(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue,
) {
    let Some(registry) = scope.get_slot::<Shared>().cloned() else {
        return;
    };
    let Ok(label) = v8::Local::<v8::String>::try_from(args.get(0)) else {
        return refuse(scope, "Invalid encoding label", true);
    };
    if label.length() > 128 {
        return refuse(scope, "Invalid encoding label", true);
    }
    let label = label.to_rust_string_lossy(scope);
    let Ok(value) = TextDecoder::new(
        &label,
        DecoderOptions {
            fatal: args.get(1).is_true(),
            ignore_bom: args.get(2).is_true(),
        },
    ) else {
        return refuse(scope, "Unknown encoding label", true);
    };
    let id = {
        let mut registry = registry.borrow_mut();
        if registry.decoders.len() >= registry.capacity || registry.next >= 9_007_199_254_740_991 {
            None
        } else {
            registry.next += 1;
            Some(registry.next)
        }
    };
    let Some(id) = id else {
        return refuse(scope, "Text decoder capacity exhausted", true);
    };
    let data = v8::Number::new(scope, id as f64);
    let Some(function) = v8::Function::builder(decode).data(data.into()).build(scope) else {
        return refuse(scope, "Text allocation failed", true);
    };
    let weak_registry = Rc::downgrade(&registry);
    let owner = v8::Weak::with_finalizer(
        scope,
        function,
        Box::new(move |_| {
            if let Some(registry) = weak_registry.upgrade() {
                registry.borrow_mut().decoders.remove(&id);
            }
        }),
    );
    let label = value.encoding();
    registry.borrow_mut().decoders.insert(
        id,
        Decoder {
            value,
            _owner: owner,
        },
    );
    let Some(label) = v8::String::new(scope, &label) else {
        return refuse(scope, "Text allocation failed", true);
    };
    rv.set(v8::Array::new_with_elements(scope, &[label.into(), function.into()]).into());
}

pub(super) fn install(scope: &mut v8::PinScope, maximum: usize, capacity: usize) -> Result<()> {
    scope.set_slot(Rc::new(RefCell::new(Registry {
        maximum,
        capacity,
        next: 0,
        decoders: HashMap::new(),
    })));
    let encoder = v8::Function::new(scope, encode).context("text encoder initialization failed")?;
    let decoder =
        v8::Function::new(scope, create_decoder).context("text decoder initialization failed")?;
    let code = v8::String::new(scope, include_str!("encoding.js"))
        .context("text encoding source allocation failed")?;
    let factory = v8::Script::compile(scope, code, None)
        .and_then(|script| script.run(scope))
        .context("text encoding initialization failed")?;
    let factory = v8::Local::<v8::Function>::try_from(factory)?;
    let undefined = v8::undefined(scope);
    factory
        .call(scope, undefined.into(), &[encoder.into(), decoder.into()])
        .context("text encoding installation failed")?;
    Ok(())
}

#[cfg(test)]
#[path = "encoding_tests.rs"]
mod tests;
