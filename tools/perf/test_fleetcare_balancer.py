# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import io
import json
import unittest
from unittest.mock import patch

import fleetcare_balancer as lb


class BalancerEvidenceTests(unittest.TestCase):
    def counters(self, count):
        return {f"node{i}": {"status":"UP", "lbtot":str(count), "hrsp_2xx":str(count)} for i in range(1,4)}

    def test_counter_deltas_distinguish_backend_selection_and_responses(self):
        before, after = self.counters(100), self.counters(110)
        after['node2']['hrsp_2xx'] = '109'
        after['node2']['hrsp_5xx'] = '1'
        result = lb.distribution(before, after)
        self.assertEqual(result['selected_total'], 30)
        self.assertEqual(result['selected_fractions'], {f'node{i}':1/3 for i in range(1,4)})
        self.assertEqual(result['nodes']['node2']['hrsp_2xx'], 9)
        self.assertEqual(result['nodes']['node2']['hrsp_5xx'], 1)

    def test_restarts_and_proxy_replays_cannot_look_like_valid_distribution(self):
        for field, value in [('lbtot','0'), ('wretr','1'), ('wredis','1')]:
            before, after = self.counters(100), self.counters(110)
            after['node1'][field] = value
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                lb.distribution(before, after)

    def test_stats_requires_all_three_backend_rows(self):
        response = io.BytesIO(b'# pxname,svname,status,lbtot\ncellar,node1,UP,100\n')
        with patch.object(lb.urllib.request, 'urlopen', return_value=response), self.assertRaises(RuntimeError):
            lb.statistics('http://fixture/stats')

    def test_unsent_and_failed_arrivals_do_not_count_as_successful_cell_coverage(self):
        manifest = {'cells':[{'path':f'/{i}', 'kind':'devices'} for i in range(3)]}
        rows = [dict(path='/0',sent_us=0,status='ok',owner='node1'),
                dict(path='/1',sent_us=None,status='generator_saturated'),
                dict(path='/2',sent_us=0,status='error:timeout')]
        result = lb.coverage(manifest, map(json.dumps, rows))
        self.assertEqual(result['by_kind']['devices'], {'expected':3, 'sent':2, 'successful':1})
        self.assertEqual(result['successful_business_owner_requests'], {'node1':1})


if __name__ == '__main__':
    unittest.main()
