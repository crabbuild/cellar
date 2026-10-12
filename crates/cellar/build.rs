fn main() {
    // This is Cargo's compiler protocol, outside the application's CLI.
    use std::io::Write;
    std::io::stdout()
        .write_all(
            format!(
                "cargo:rustc-env=CELLAR_NATIVE_TARGET={}\n",
                std::env::var("TARGET").unwrap()
            )
            .as_bytes(),
        )
        .unwrap();
}
