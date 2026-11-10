# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Deployment inputs must satisfy the physical group's transport identity."""
import unittest

from managed_group_drill import ManagedQualification


class ManagedDrillTests(unittest.TestCase):
    def test_container_peer_listeners_bind_the_advertised_identity(self):
        driver=object.__new__(ManagedQualification)
        driver.config={key:'fixture' for key in ('endpoint','bucket','prefix','access','secret','token')}
        driver.config['fence_public']=[1]*32
        driver.model={'bootstrap':{'connection':{}}}
        row={'node':'01'*16,'disk':'02'*16,'process':'03'*16,
             'label':'managed-fixture','ip':'172.28.1.11'}
        config=driver.node_config(row,False)
        self.assertEqual(config['bind'],row['ip'])
        self.assertEqual(config['bind'],config['advertise'])


if __name__=='__main__':
    unittest.main()
