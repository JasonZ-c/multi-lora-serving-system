"""One run/config/trace; phase batching first, then incremental locality.

python3.12 scripts/screen.py batching
python3.12 scripts/screen.py locality --wait 60
"""
import argparse
import dataclasses
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark import run
from scheduler import Config
from run import summary

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('phase', choices=('batching','locality'))
parser.add_argument('--wait', type=int, default=60)
args=parser.parse_args()
base=Config(controller_margin=.25,response_margin=.25,record_events=False,
            starvation_threshold=60,
            demand_eviction=False)
variants=[]
if args.phase=='batching':
    # One FIFO reference, then four representative Fair windows.
    variants.append(('fifo-w30',dataclasses.replace(base,policy='fifo',
                     max_batch_wait=30,starvation_threshold=60)))
    for wait in (0,30,60,120):
        variants.append((f'fair-w{wait}',dataclasses.replace(base,policy='fair',
                         max_batch_wait=wait)))
else:
    base=dataclasses.replace(base,policy='locality',max_batch_wait=args.wait)
    variants=[('joint',base),('demand',dataclasses.replace(base,demand_eviction=True)),
              ('prefetch',dataclasses.replace(base,demand_eviction=True,prefetch=True)),
              ('replication',dataclasses.replace(base,demand_eviction=True,prefetch=True,replication=True))]
for label,config in variants:
    for pattern in (80,50):
        target=f'results/screen/{label}-p{pattern}.json'
        card=run(f'results/traces/screen-p{pattern}-seed17.jsonl.gz',config,target,raw=False)
        print(target,summary(card),flush=True)
        c=card['conservation']
        if c['failed'] or c['deadline_misses'] or c['outstanding']:
            raise SystemExit('protected invariants failed')
