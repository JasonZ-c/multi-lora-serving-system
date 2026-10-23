"""Two focused engine checks; no acceleration or threshold sweep.

Execute from the repository root: python3.12 scripts/calibrate.py
Writes calibration cards under results/calibration.
"""
import gzip
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from benchmark import run
from scheduler import Config
from run import summary

# Reuse saved inputs, including their compressed bytes and trace hashes.
result = run('results/traces/speed1-cold.jsonl.gz',
             Config(max_batch_wait=0, starvation_threshold=60,
                    controller_margin=.25, response_margin=.25),
             'results/calibration/speed1-cold.json', backend_kind='original', speed=1)
print('speed1-cold', summary(result), result['arrival_lateness'], flush=True)

result = run('results/traces/base-saturation.jsonl.gz',
             Config(max_batch_wait=0, starvation_threshold=60,
                    controller_margin=1.5, response_margin=1.5,
                    tenant_ticket_cap=1024),
             'results/calibration/base-saturation-final.json', backend_kind='original', speed=5)
with gzip.open('results/calibration/base-saturation-final.events.jsonl.gz', 'rt') as source:
    events = [json.loads(line) for line in source]
span = (max(e['ts'] for e in events if e['event'] == 'batch_end') -
        min(e['ts'] for e in events if e['event'] == 'batch_start'))
print('base-saturation fleet rps', result['conservation']['completed'] / span, flush=True)
