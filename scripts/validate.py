"""Full-scale validation afterscreening, seed17/singleruns.

python3.12 scripts/validate.py locality
python3.12 scripts/validate.py final-virtual
python3.12 scripts/validate.py final-original --speed 5
python3.12 scripts/validate.py replay

Historical discarded policies require their source ZIP listed in the card.
"""
import argparse
import dataclasses
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from benchmark import run
from scheduler import Config
from run import summary

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('phase',choices=('locality','final-virtual','final-original','replay'))
parser.add_argument('--speed',type=float,default=5)
args=parser.parse_args()
base=Config(max_batch_wait=60,starvation_threshold=60,controller_margin=.25,
            response_margin=.25,demand_eviction=False,record_events=False)
if args.phase=='locality':
    base=dataclasses.replace(base,policy='locality')
    variants=[('joint',base),('demand',dataclasses.replace(base,demand_eviction=True)),
              ('prefetch',dataclasses.replace(base,demand_eviction=True,prefetch=True)),
              ('replication',dataclasses.replace(base,demand_eviction=True,prefetch=True,replication=True))]
else:
    import json
    final=Config(**json.loads(Path('final_config.json').read_text()))
    base=dataclasses.replace(final,policy='fair',demand_eviction=False,
                             prefetch=False,replication=False)
    variants=[('fifo',dataclasses.replace(base,policy='fifo')),
              ('fair',dataclasses.replace(base,policy='fair')),('locality',final)]
for label,config in variants:
    for pattern in (80,50):
        if args.phase=='locality':
            directory='results/full-locality'
        elif args.phase=='replay':
            directory='results/replay'
        else:
            directory='results/'+args.phase
        target=f'{directory}/{label}-p{pattern}.json'
        replay=f'results/final-virtual/fifo-p{pattern}.requests.jsonl.gz' if args.phase=='replay' else None
        card=run(f'results/traces/full-p{pattern}-seed17.jsonl.gz',config,target,
                 backend_kind='original' if args.phase=='final-original' else 'virtual',
                 speed=args.speed,replay=replay,raw=args.phase!='locality')
        print(target,summary(card),flush=True)
        c=card['conservation']
        if args.phase!='replay' and (c['failed'] or c['deadline_misses'] or c['outstanding']):
            raise SystemExit('protected invariants failed')
