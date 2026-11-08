// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[tokio::test]
async fn close_retains_service_admission_until_the_actual_worker_exits() {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .run(async {
            let services = Services::new(128, 1);
            let worker = services
                .register(ControlServiceKind::Partition, 64)
                .unwrap();
            let stop = worker.stop_receiver();
            assert!(matches!(
                services.register(ControlServiceKind::Partition, 64),
                Err(FleetCatalogError::Overloaded)
            ));
            assert!(matches!(
                services.register(ControlServiceKind::Restart, 64),
                Err(FleetCatalogError::Overloaded)
            ));
            let mut closing = Box::pin(services.close());
            assert!(
                crate::asyncrt::timeout(Duration::from_millis(30), &mut closing)
                    .await
                    .is_err()
            );
            assert!(*stop.borrow());
            assert_eq!(services.memory.usage(), (64, 1));
            drop(closing);
            // Cancelling a close waiter neither removes the live worker nor reopens
            // admission. The independently owned cleanup can resume the same join.
            assert!(matches!(
                services.register(ControlServiceKind::Restart, 1),
                Err(FleetCatalogError::Closed)
            ));
            assert_eq!(services.memory.usage(), (64, 1));
            drop(worker);
            services.close().await;
            assert_eq!(services.memory.usage(), (0, 0));
        })
        .await
        .unwrap();
    domain.shutdown().await;
}
