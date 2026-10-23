#!/usr/bin/env python3
"""Standard-library commands for the product and reproducible experiments."""
from __future__ import annotations

import argparse
import dataclasses
import json
import tempfile
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def package(output, root=ROOT):
    output, root = Path(output).resolve(), Path(root).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in sorted(root.rglob('*')):
            relative = path.relative_to(root)
            if (not path.is_file() or path.resolve() == output or
                any(part in {'.git', '__pycache__', '.venv', 'build', 'dist'} for part in relative.parts) or
                path.suffix in {'.pyc', '.db', '.sqlite', '.sqlite3'} or
                path.name.endswith(('-wal', '-shm', '.requests.jsonl.gz', '.events.jsonl.gz')) or
                path.name == '.DS_Store'):
                continue
            archive.write(path, str(Path('multi-lora-serving') / relative))
    return output


def configuration(args):
    from scheduler import Config
    values = json.loads(Path(args.config).read_text()) if getattr(args, 'config', None) else {}
    if 'config' in values:  # Every result card is also a reproducible config.
        values = values['config']
    for field in ('policy', 'max_batch_wait', 'slo', 'controller_margin', 'response_margin',
                  'prefetch', 'replication', 'demand_eviction'):
        value = getattr(args, field, None)
        if value is not None:
            values[field] = value
    values.setdefault('starvation_threshold', 60)
    return Config(**values)


def summary(card):
    counts = card['conservation']
    return {'backend': card['backend'], 'policy': card['config']['policy'],
            'wait': card['config']['max_batch_wait'], 'accepted': counts['accepted'],
            'completed': counts['completed'], 'reject_pct': round(100 * card['rejection_rate'], 2),
            'misses': counts['deadline_misses'], 'failed': counts['failed'],
            'max_gap': round(card['max_backlogged_service_gap'], 2),
            'burst_goodput': round(card['windows']['burst']['deadline_goodput'], 2),
            'recovery_goodput': round(card['windows']['recovery']['deadline_goodput'], 2),
            'wall_seconds': round(card['wall_seconds'], 2)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    trace = sub.add_parser('trace', help='save deterministic seed-17 input')
    trace.add_argument('--stage', choices=('screen', 'full'), default='full')
    trace.add_argument('--pattern', choices=(80, 50), type=int, required=True)
    trace.add_argument('--output', required=True)
    bench = sub.add_parser('bench', help='run a protected comparison or accepted-work replay')
    bench.add_argument('--trace', required=True)
    bench.add_argument('--output', required=True)
    bench.add_argument('--backend', choices=('virtual', 'original'), default='virtual')
    bench.add_argument('--speed', type=float, default=10)
    bench.add_argument('--replay', help='requests.jsonl.gz accepted cohort; disables deadline guard')
    bench.add_argument('--no-raw', action='store_true')
    bench.add_argument('--policy', choices=('fifo', 'fair', 'locality'))
    bench.add_argument('--batch-wait', dest='max_batch_wait', type=float)
    for field in ('prefetch', 'replication', 'demand-eviction'):
        dest = field.replace('-', '_')
        bench.add_argument('--' + field, dest=dest, action='store_true', default=None)
        bench.add_argument('--no-' + field, dest=dest, action='store_false')
    for field in ('controller-margin', 'response-margin', 'slo'):
        bench.add_argument('--' + field, type=float)
    bench.add_argument('--config')
    provision = sub.add_parser('provision', help='provision a customer and print a new API key')
    provision.add_argument('--db', required=True)
    provision.add_argument('--customer', required=True)
    serve = sub.add_parser('serve', help='serve authenticated HTTP backed by original engines')
    serve.add_argument('--db', required=True)
    serve.add_argument('--host', default='127.0.0.1')
    serve.add_argument('--port', type=int, default=8080)
    serve.add_argument('--speed', type=float, default=1)
    serve.add_argument('--config', default=str(ROOT / 'final_config.json'))
    demo = sub.add_parser('demo', help='register, release, infer, poll and roll back locally')
    demo.add_argument('--speed', type=float, default=20)
    report = sub.add_parser('report', help='print compact comparison cards')
    report.add_argument('files', nargs='+')
    audit = sub.add_parser('audit', help='independently recheck saved raw request and engine evidence')
    audit.add_argument('files', nargs='+')
    bundle = sub.add_parser('package', help='archive source, configs, and result cards')
    bundle.add_argument('--output', default=str(ROOT / 'dist' / 'multi-lora-serving.zip'))
    args = parser.parse_args(argv)
    if args.command == 'trace':
        from workload import save_trace, STAGES
        print(save_trace(args.output, args.pattern / 100, STAGES[args.stage]))
    elif args.command == 'bench':
        from benchmark import run
        card = run(args.trace, configuration(args), args.output, args.backend, args.speed,
                   args.replay, raw=not args.no_raw)
        print(json.dumps(summary(card), sort_keys=True), flush=True)
        c = card['conservation']
        if args.replay is None and (c['failed'] or c['deadline_misses'] or c['outstanding']):
            raise SystemExit('protected accepted-work invariant failed; see result card')
    elif args.command == 'provision':
        from store import Store
        store = Store(args.db)
        try:
            print(store.provision_customer(args.customer))
        finally:
            store.close()
    elif args.command == 'serve':
        from backends import ThreadedBackend
        from scheduler import Scheduler
        from service import ProductService
        from store import Store
        from http_api import make_server
        config = configuration(args)
        store = Store(args.db)
        backend = ThreadedBackend(config.replicas, speed=args.speed, origin=store.clock_origin(args.speed))
        service = ProductService(store, Scheduler(config, backend), slo=config.slo,
                                 poll_interval=min(.005, .05 / args.speed))
        server = make_server(service, args.host, args.port)
        service.start_worker()
        print(f'HTTP listening on http://{args.host}:{server.server_port}', flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
            service.close()
            store.close()
    elif args.command == 'demo':
        from backends import ThreadedBackend
        from scheduler import Config, Scheduler
        from service import ProductService
        from store import Store
        with tempfile.TemporaryDirectory() as temporary:
            store = Store(str(Path(temporary) / 'demo.db'))
            key = store.provision_customer('acme')
            config = Config(max_batch_wait=0, controller_margin=.5, response_margin=.5)
            backend = ThreadedBackend(config.replicas, speed=args.speed, origin=time.time())
            service = ProductService(store, Scheduler(config, backend), slo=30,
                                     poll_interval=.001)
            try:
                service.register_version(key, 'support', 'v1', 's3://demo/v1')
                service.release(key, 'support', 'support', 'v1', 0, 'first')
                _, request = service.infer(key, {'model': 'endpoint:support', 'prompt': 'hello'}, 'demo')
                service.register_version(key, 'support', 'v2', 's3://demo/v2')
                service.release(key, 'support', 'support', 'v2', 1, 'promote')
                service.release(key, 'support', 'support', 'v1', 2, 'rollback')
                service.start_worker()
                until = time.monotonic() + 10
                while time.monotonic() < until:
                    request = service.get_request(key, request['id'])
                    if request['status'] in ('completed', 'failed'):
                        break
                    time.sleep(.005)
                print(json.dumps(request, indent=2))
                if request['status'] != 'completed' or request['slo_violation']:
                    raise SystemExit('demo did not complete within its deadline')
            finally:
                service.close()
                store.close()
    elif args.command == 'report':
        for file in args.files:
            print(Path(file).name, json.dumps(summary(json.loads(Path(file).read_text())), sort_keys=True))
    elif args.command == 'audit':
        from benchmark import audit_card
        for file in args.files:
            print(Path(file).name, json.dumps(audit_card(file), sort_keys=True))
    else:
        print(package(args.output))


if __name__ == '__main__':
    main()
