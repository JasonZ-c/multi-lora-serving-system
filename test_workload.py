import tempfile
import unittest
from collections import Counter
from pathlib import Path


class Workloads(unittest.TestCase):
    def test_manifest_joint_marginals_and_scale(self):
        import workload
        for pattern in (0.8, 0.5):
            manifest = workload.manifest(pattern)
            self.assertEqual(len(manifest['customers']), 50)
            self.assertEqual(len(manifest['adapters']), 200)
            owners = Counter(a['customer'] for a in manifest['adapters'])
            self.assertTrue(all(1 <= n <= 10 for n in owners.values()))
            self.assertEqual(sum(a['hot'] for a in manifest['adapters']), 10)
            weights = manifest['probabilities']
            self.assertAlmostEqual(sum(weights), 1)
            self.assertAlmostEqual(sum(p for p, a in zip(weights, manifest['adapters']) if a['hot']), pattern)
            self.assertAlmostEqual(sum(p for p, a in zip(weights, manifest['adapters']) if a['customer'] == 'c00'), .4)
            self.assertAlmostEqual(sum(p for p, a in zip(weights, manifest['adapters']) if a['customer'] == 'c49'), .0002)

    def test_saved_trace_is_deterministic_and_rates_match(self):
        import workload
        with tempfile.TemporaryDirectory() as tmp:
            first, second = Path(tmp) / 'one.gz', Path(tmp) / 'two.gz'
            h1 = workload.save_trace(first, .8, (1, 1, 1))
            h2 = workload.save_trace(second, .8, (1, 1, 1))
            self.assertEqual(h1, h2)
            header, rows = workload.read_trace(first)
            self.assertEqual(header['seed'], 17)
            self.assertEqual(len(rows), 1500)
            self.assertEqual(sum(t < 1 for t, _, _ in rows), 250)
            self.assertEqual(sum(1 <= t < 2 for t, _, _ in rows), 1000)
            self.assertEqual(sum(2 <= t < 3 for t, _, _ in rows), 250)

    def test_empirical_marginals_and_popularity(self):
        import workload
        for pattern in (.8, .5):
            spec = workload.manifest(pattern)
            rows = list(workload.generate(spec, (30, 60, 60)))
            counts = Counter(customer for _, customer, _ in rows)
            hot = {a['id'] for a in spec['adapters'] if a['hot']}
            self.assertAlmostEqual(counts['c00'] / len(rows), .4, delta=.01)
            self.assertAlmostEqual(sum(a in hot for _, _, a in rows) / len(rows), pattern, delta=.01)
            self.assertEqual(set(counts), set(spec['customers']))
