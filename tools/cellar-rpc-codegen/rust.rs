// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use prost_build::{Service, ServiceGenerator};
use std::{cell::RefCell, fmt::Write, path::Path, rc::Rc};

pub fn generate(
    directory: &Path,
    descriptor: &prost_types::FileDescriptorSet,
    bytes: &[u8],
) -> Result<()> {
    let failure = Rc::new(RefCell::new(None));
    prost_build::Config::new()
        .out_dir(directory)
        .prost_path("::cellar::sdk::rpc::prost")
        .prost_types_path("::cellar::sdk::rpc::prost_types")
        .service_generator(Box::new(Generator {
            descriptor: format!("{bytes:?}"),
            failure: failure.clone(),
        }))
        .compile_fds(descriptor.clone())?;
    if let Some(error) = failure.take() {
        return Err(error);
    }
    super::format::generated_rust(directory)
}

pub struct Generator {
    descriptor: String,
    failure: Rc<RefCell<Option<anyhow::Error>>>,
}

impl ServiceGenerator for Generator {
    fn finalize(&mut self, buffer: &mut String) {
        match super::graph::implementations(buffer) {
            Ok(visitors) => buffer.push_str(&visitors),
            Err(error) => *self.failure.borrow_mut() = Some(error),
        }
    }

    fn generate(&mut self, service: Service, out: &mut String) {
        // Names/types are supplied by prost's language identifier conversion;
        // the descriptor has already passed Cellar's profile validator.
        let name = &service.name;
        let contract = format!("__cellar_{}_contract", service.proto_name.to_lowercase());
        writeln!(out, "\nfn {contract}() -> Result<::std::sync::Arc<::cellar::sdk::rpc::Contract>, ::cellar::sdk::rpc::Failure> {{
            static CONTRACT: ::std::sync::OnceLock<Result<::std::sync::Arc<::cellar::sdk::rpc::Contract>, String>> = ::std::sync::OnceLock::new();
            match CONTRACT.get_or_init(|| ::cellar::sdk::rpc::Contract::from_descriptor(&{}, ::cellar::sdk::rpc::Limits::default()).map(::std::sync::Arc::new).map_err(|e| e.to_string())) {{
                Ok(contract) => Ok(contract.clone()),
                Err(_) => Err(::cellar::sdk::rpc::Failure::new(::cellar::sdk::rpc::Code::Internal)),
            }}
        }}", self.descriptor).expect("writing to a string");
        writeln!(out, "pub trait {name}Handler: Send + Sync + 'static {{")
            .expect("writing to a string");
        for method in &service.methods {
            writeln!(out, "fn {}(self: ::std::sync::Arc<Self>, context: ::cellar::sdk::Context, request: {}) -> ::cellar::sdk::rpc::HandlerFuture<{}>;", method.name, method.input_type, method.output_type).expect("writing to a string");
        }
        writeln!(out, "}}\n#[derive(Clone)]\npub struct {name}Client {{ inner: ::cellar::sdk::rpc::Client }}\nimpl {name}Client {{
            pub fn bind(context: &::cellar::sdk::Context, binding: &str, name: &str) -> Result<Self, ::cellar::sdk::rpc::Failure> {{
                Ok(Self {{ inner: ::cellar::sdk::rpc::Client::bind(context, binding, name, {contract}()?)? }})
            }}").expect("writing to a string");
        for method in &service.methods {
            let wire = format!(
                "{}.{}/{}",
                service.package, service.proto_name, method.proto_name
            );
            writeln!(out, "pub fn {}(&self, request: {}) -> ::cellar::sdk::rpc::HandlerFuture<{}> {{ self.{}_with_options(request, ::cellar::sdk::rpc::CallOptions::default()) }}
                pub fn {}_with_options(&self, request: {}, options: ::cellar::sdk::rpc::CallOptions) -> ::cellar::sdk::rpc::HandlerFuture<{}> {{ self.inner.call({wire:?}, request, options) }}",
                method.name, method.input_type, method.output_type, method.name,
                method.name, method.input_type, method.output_type).expect("writing to a string");
        }
        writeln!(out, "}}\npub struct {name}Registration;\nimpl {name}Registration {{
            pub fn register<T: {name}Handler>(mut router: ::cellar::sdk::rpc::Router, handler: ::std::sync::Arc<T>) -> Result<::cellar::sdk::rpc::Router, ::cellar::sdk::rpc::Failure> {{
                let contract = {contract}()?;").expect("writing to a string");
        for method in &service.methods {
            let wire = format!(
                "{}.{}/{}",
                service.package, service.proto_name, method.proto_name
            );
            // Prost maps Empty to Rust's unit type. Decode it for validation,
            // then use the literal without emitting meaningless unit bindings.
            let (input_binding, input) = if method.input_type == "()" {
                ("", "()")
            } else {
                ("let request = ", "request")
            };
            let (output_binding, response) = if method.output_type == "()" {
                ("", "()")
            } else {
                ("let response = ", "response")
            };
            writeln!(out, "{{ let handler = handler.clone();
                router = router.method_with_limits(contract.clone(), {wire:?}, move |context, bytes, limits| {{
                    let handler = handler.clone();
                    Box::pin(async move {{
                        {input_binding}<{} as ::cellar::sdk::rpc::prost::Message>::decode(bytes.as_slice()).map_err(|_| ::cellar::sdk::rpc::Failure::new(::cellar::sdk::rpc::Code::InvalidArgument))?;
                        {output_binding}handler.{}(context, {input}).await?;
                        ::cellar::sdk::rpc::encode_message({response}, limits).map_err(|_| ::cellar::sdk::rpc::Failure::new(::cellar::sdk::rpc::Code::InvalidResponse))
                    }})
                }}).map_err(|_| ::cellar::sdk::rpc::Failure::new(::cellar::sdk::rpc::Code::InvalidArgument))?;
            }}", method.input_type, method.name).expect("writing to a string");
        }
        writeln!(out, "router.validate().map_err(|_| ::cellar::sdk::rpc::Failure::new(::cellar::sdk::rpc::Code::InvalidArgument))?;
            Ok(router)
        }} }}").expect("writing to a string");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::{DescriptorProto, FileDescriptorProto};

    #[test]
    fn files_without_services_and_shared_packages_receive_graph_guards_once() {
        let files = [
            ("a.proto", "shared", "First"),
            ("b.proto", "shared", "Second"),
            ("c.proto", "other", "Third"),
        ];
        let requests = files
            .into_iter()
            .map(|(file, package, name)| {
                (
                    prost_build::Module::from_protobuf_package_name(package),
                    FileDescriptorProto {
                        name: Some(file.into()),
                        package: Some(package.into()),
                        syntax: Some("proto3".into()),
                        message_type: vec![DescriptorProto {
                            name: Some(name.into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )
            })
            .collect();
        let failure = Rc::new(RefCell::new(None));
        let buffers = prost_build::Config::new()
            .service_generator(Box::new(Generator {
                descriptor: String::new(),
                failure: failure.clone(),
            }))
            .generate(requests)
            .unwrap();
        assert!(failure.borrow().is_none());
        assert_eq!(buffers.len(), 2);
        let shared = &buffers[&prost_build::Module::from_protobuf_package_name("shared")];
        assert_eq!(shared.matches("MessageGraph for First").count(), 1);
        assert_eq!(shared.matches("MessageGraph for Second").count(), 1);
        assert_eq!(
            buffers[&prost_build::Module::from_protobuf_package_name("other")]
                .matches("MessageGraph for Third")
                .count(),
            1
        );
    }
}
