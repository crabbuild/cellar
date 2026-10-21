//! Verify actual GNU loader precedence with the same arguments used by builds.
use super::*;
use std::process::Command;

fn command(command: &mut Command) -> Result<std::process::Output> {
    let output = command.output()?;
    ensure!(
        output.status.success(),
        "loader fixture command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output)
}

#[test]
fn verified_rpath_wins_over_ambient_same_soname_library() -> Result<()> {
    let work = tempfile::tempdir()?;
    let bound = work.path().join("verified library, with spaces");
    let ambient = work.path().join("ambient");
    std::fs::create_dir(&bound)?;
    std::fs::create_dir(&ambient)?;
    let source = work.path().join("library.c");
    for (directory, value) in [(&bound, 42), (&ambient, 99)] {
        std::fs::write(
            &source,
            format!("int selected_runtime(void) {{ return {value}; }}\n"),
        )?;
        command(
            Command::new("cc")
                .args([
                    "-shared",
                    "-fPIC",
                    "-Wl,-soname,libcellar_loader_fixture.so",
                ])
                .arg(&source)
                .arg("-o")
                .arg(directory.join("libcellar_loader_fixture.so")),
        )?;
    }
    let main = work.path().join("main.c");
    std::fs::write(&main, "extern int selected_runtime(void); int main(void) { return selected_runtime() == 42 ? 0 : 1; }\n")?;
    let arguments = rpath_arguments(
        "x86_64-unknown-linux-gnu",
        [bound.clone()].into_iter().collect(),
    )?;
    let (pairs, remainder) = arguments.as_chunks::<2>();
    anyhow::ensure!(remainder.is_empty(), "unpaired Rust linker argument");
    let linker: Vec<_> = pairs
        .iter()
        .map(|pair| {
            assert_eq!(pair[0], "-C");
            pair[1].strip_prefix("link-arg=").unwrap()
        })
        .collect();
    let probe = work.path().join("probe");
    command(
        Command::new("cc")
            .arg(&main)
            .arg("-L")
            .arg(&bound)
            .arg("-lcellar_loader_fixture")
            .args(linker)
            .arg("-o")
            .arg(&probe),
    )?;
    command(Command::new(&probe).env("LD_LIBRARY_PATH", &ambient))?;
    // With no bound library present, the ambient impostor is observable and
    // the probe must refuse it, rather than silently passing the first case.
    std::fs::rename(
        bound.join("libcellar_loader_fixture.so"),
        bound.join("hidden.so"),
    )?;
    ensure!(
        !Command::new(&probe)
            .env("LD_LIBRARY_PATH", &ambient)
            .status()?
            .success(),
        "loader probe did not distinguish the two libraries"
    );
    for directory in ["/tmp/path:split", "/tmp/$ORIGIN/library"] {
        ensure!(rpath_arguments(
            "x86_64-unknown-linux-gnu",
            [PathBuf::from(directory)].into_iter().collect()
        )
        .is_err());
    }
    Ok(())
}
