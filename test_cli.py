import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path


class Commands(unittest.TestCase):
    def test_help_lists_delivery_commands(self):
        result = subprocess.run([sys.executable, 'run.py', '--help'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        for command in ('trace', 'bench', 'serve', 'provision', 'demo', 'report', 'package'):
            self.assertIn(command, result.stdout)

    def test_package_excludes_cache_and_database(self):
        from run import package
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / 'source'
            root.mkdir()
            (root / 'README.md').write_text('run me')
            (root / 'app.py').write_text('print(1)')
            (root / 'secret.db').write_text('private')
            cache = root / '__pycache__'
            cache.mkdir()
            (cache / 'a.pyc').write_text('ignore')
            dest = Path(tmp) / 'delivery.zip'
            package(dest, root)
            with zipfile.ZipFile(dest) as archive:
                self.assertEqual(sorted(archive.namelist()), ['multi-lora-serving/README.md', 'multi-lora-serving/app.py'])

    def test_batch_wait_overrides_keep_default_starvation_guard_fixed_at_sixty(self):
        import argparse
        from run import configuration
        for wait in (0,30,60,120):
            with self.subTest(batch_wait=wait):
                config = configuration(argparse.Namespace(max_batch_wait=wait))
                self.assertEqual(config.max_batch_wait,wait)
                self.assertEqual(config.starvation_threshold,60)

    def test_result_card_config_preserves_custom_threshold(self):
        import argparse
        import json
        from run import configuration
        with tempfile.TemporaryDirectory() as tmp:
            card = Path(tmp) / 'card.json'
            card.write_text(json.dumps({'config': {'max_batch_wait': 10, 'starvation_threshold': 80}}))
            self.assertEqual(configuration(argparse.Namespace(config=str(card))).starvation_threshold, 80)
            overridden = configuration(argparse.Namespace(config=str(card),max_batch_wait=30))
            self.assertEqual((overridden.max_batch_wait,overridden.starvation_threshold),(30,80))
