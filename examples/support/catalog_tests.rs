//! Validate tutorial configurations against the real native manifest contract.

// Fixture reads belong to test tooling, outside the node execution boundary.
#![allow(clippy::disallowed_methods)]
use super::*;
use cellar::native_deploy::{current_target, executable_identity, Project};
use std::path::Path;

#[test]
fn focused_projects_validate_and_have_compiled_factories() -> Result<()> {
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples");
    let catalog: serde_json::Value =
        serde_json::from_slice(&std::fs::read(examples.join("catalog.json"))?)?;
    let temporary = tempfile::tempdir()?;
    let fixture = temporary.path().join("artifact");
    std::fs::write(&fixture, b"manifest validation fixture")?;
    let artifact = executable_identity(&fixture, current_target())?;
    let mut paths = vec!["native-examples-project.json".to_string()];
    for entry in catalog["examples"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("catalog examples missing"))?
    {
        if let Some(project) = entry["project"].as_str() {
            paths.push(project.into());
        }
    }
    paths.sort();
    paths.dedup();
    for path in paths {
        let config_file = examples.join(&path);
        let project: Project = serde_json::from_slice(&std::fs::read(&config_file)?)?;
        if project.binary != "cellar-native-examples" {
            continue;
        }
        assert!(
            SCRIPTS.contains(&project.primary.script_name.as_str())
                || matches!(
                    project.primary.script_name.as_str(),
                    "native-examples" | "team-api"
                ),
            "unregistered project: {path}"
        );
        for mut config in std::iter::once(project.primary).chain(project.cohosted) {
            let app = match config.script_name.as_str() {
                "team-api" => services::api()?,
                "team-ledger" => services::ledger()?,
                _ => application()?,
            };
            assert_eq!(config.application_version, "v1", "{path}");
            for class in &config.cells {
                assert!(app.supports_cell(class), "missing class {class} in {path}");
            }
            for binding in &config.bindings {
                if binding["type"] == "workflow" {
                    assert!(
                        app.supports_workflow(binding["class_name"].as_str().unwrap_or_default()),
                        "missing workflow in {path}"
                    );
                }
            }
            if let Some(assets) = config.assets.take() {
                // Asset staging accepts only project-local paths.
                let directory = assets.directory;
                assert!(
                    !directory.is_absolute()
                        && !directory
                            .components()
                            .any(|part| matches!(part, std::path::Component::ParentDir)),
                    "invalid assets in {path}"
                );
                assert!(
                    config_file.parent().unwrap().join(directory).is_dir(),
                    "missing assets in {path}"
                );
            }
            config.manifest(vec![artifact.clone()])?;
        }
    }
    Ok(())
}
