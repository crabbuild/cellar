fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        cc::Build::new()
            .file("cli/native_deploy/python_environment/prerequisites/shared_cache.c")
            .warnings(true)
            .compile("cellar_python_shared_cache");
    }
    // This is Cargo's compiler protocol, outside the application's CLI.
    use std::io::Write;
    std::io::stdout()
        .write_all(
            format!(
                "cargo:rerun-if-changed=build.rs\ncargo:rerun-if-changed=cli/native_deploy/python_environment/prerequisites/shared_cache.c\ncargo:rustc-env=CELLAR_NATIVE_TARGET={}\n",
                std::env::var("TARGET").unwrap()
            )
            .as_bytes(),
        )
        .unwrap();
}
