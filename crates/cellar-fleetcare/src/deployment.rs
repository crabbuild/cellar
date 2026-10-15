use crate::config::*;
use cellar::engine_api::{Compat, QueueBinding, WorkerConfigOptions, WorkflowBinding};
use cellar::fleet::LoadedDeployment;
use cellar::sdk::DeploymentGraph;

pub fn graph(config: &AppConfig) -> DeploymentGraph {
    let counts = config.profile.counts();
    let mut sql = Vec::new();
    let mut kv = Vec::new();
    let mut queues = Vec::new();
    let mut consumers = Vec::new();
    for tenant in 0..counts.tenants {
        for partition in 0..3 {
            sql.push((
                binding("SQL", tenant, partition),
                sql_identity(tenant, partition),
            ));
        }
        kv.push((binding("KV", tenant, 0), kv_identity(tenant)));
    }
    for pair in 0..counts.queues / 2 {
        for dead in [false, true] {
            let queue = queue_identity(pair, dead);
            queues.push(QueueBinding {
                environment: binding(if dead { "DLQ" } else { "Q" }, pair, 0),
                queue: queue.clone(),
                delivery_delay: 0,
            });
            consumers.push(cellar::protocol::QueueConsumerConfig {
                queue,
                max_batch_size: 20,
                max_batch_timeout: 1,
                max_retries: if dead { 10 } else { 2 },
                dead_letter_queue: (!dead).then(|| queue_identity(pair, true)),
                max_concurrency: Some(4),
                retry_delay: Some(if dead { 5 } else { 1 }),
            });
        }
    }
    DeploymentGraph::single(LoadedDeployment {
        event_streams: vec![],
        native: None,
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: SCRIPT.into(),
            do_classes: vec![
                "Device".into(),
                "WorkOrderBook".into(),
                "Inventory".into(),
                cellar::deploy::SQL_CLASS.into(),
                cellar::deploy::KV_CLASS.into(),
                cellar::deploy::QUEUE_CLASS.into(),
                cellar::deploy::workflow_class(SCRIPT),
            ],
            bindings: vec![
                ("DEVICES".into(), "Device".into()),
                ("BOOKS".into(), "WorkOrderBook".into()),
                ("INVENTORY".into(), "Inventory".into()),
            ],
            blob_bindings: vec![("ATTACHMENTS".into(), "fleetcare-attachments".into())],
            sql_bindings: sql,
            kv_bindings: kv,
            queue_bindings: queues,
            stream_bindings: vec![],
            queue_consumers: consumers,
            workflow_bindings: vec![WorkflowBinding {
                environment: "DISPATCH".into(),
                workflow: "dispatch".into(),
                class: "Dispatch".into(),
                settings: Default::default(),
            }],
            vars: Vec::new(),
            node: String::new(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: SCRIPT.into(),
        version: VERSION.into(),
        prefix: "fleetcare/v1".into(),
        asset_binding: None,
        loader_bindings: vec!["DIAGNOSTICS".into()],
        assets: None,
        services: vec![("REPORTS".into(), SCRIPT.into(), Some("Reports".into()))],
        crons: vec!["*/1 * * * *".into()],
        cron_revision: None,
        containers: config
            .container_image
            .as_ref()
            .map(|image| {
                vec![cellar::container::ContainerSpec {
                    class_name: "Device".into(),
                    image: image.to_string(),
                    instance_type: Some("lite".into()),
                    max_instances: Some(1),
                    runtime: None,
                }]
            })
            .unwrap_or_default(),
        fence_image: config.fence_image.as_ref().map(|image| image.to_string()),
        container_images: Vec::new(),
    })
}
