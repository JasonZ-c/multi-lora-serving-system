"""Saved fleet-scale traffic with jointly controlled customer and adapter skew.

A row is (ingress seconds, customer, immutable adapter identity). Seed 17 is
fixed by default; compressed files have deterministic headers and bytes.
"""
from __future__ import annotations

import bisect
import gzip
import hashlib
import itertools
import json
import random
from pathlib import Path

STAGES = {'screen': (30, 60, 60), 'full': (120, 600, 600)}
RATES = (250, 1000, 250)


def manifest(hot_fraction: float, seed: int = 17) -> dict:
    if not .0 < hot_fraction <= .84:
        raise ValueError('hot fraction must be in (0, .84]')
    customers = [f'c{i:02d}' for i in range(50)]
    sizes = [10] + [4] * 44 + [3] * 4 + [2]
    # Customers 0..6 own the hot set, with 84% of all demand. This makes
    # both 80% and 50% hot marginals compatible with identical ownership
    # and identical per-customer demand, including a 40% largest customer.
    head = [1 / (i + 1) for i in range(6)]
    tail = [1 / (i + 1) for i in range(42)]
    customer_mass = [.4] + [.44 * w / sum(head) for w in head]
    customer_mass += [.1598 * w / sum(tail) for w in tail] + [.0002]
    adapters, probabilities = [], []
    for i, (customer, count, mass) in enumerate(zip(customers, sizes, customer_mass)):
        hot_count = 4 if i == 0 else (1 if i <= 6 else 0)
        hot_mass = mass * hot_fraction / .84 if hot_count else 0
        for j in range(count):
            hot = j < hot_count
            adapters.append({'id': f'{customer}/a{j:02d}@v1', 'customer': customer,
                             'name': f'a{j:02d}', 'version': 'v1', 'hot': hot})
            probabilities.append(hot_mass / hot_count if hot else
                                 (mass - hot_mass) / (count - hot_count))
    return {'schema': 1, 'seed': seed, 'hot_fraction': hot_fraction,
            'customers': customers, 'adapters': adapters, 'probabilities': probabilities,
            'rates': list(RATES)}


def generate(spec: dict, durations: tuple[float, float, float]):
    rng = random.Random(spec['seed'])
    cumulative = list(itertools.accumulate(spec['probabilities']))
    start = 0.0
    for duration, rate in zip(durations, RATES):
        for i in range(round(duration * rate)):
            adapter = spec['adapters'][min(bisect.bisect(cumulative, rng.random()), 199)]
            yield (round(start + i / rate, 9), adapter['customer'], adapter['id'])
        start += duration


def trace_hash(path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as source:
        for chunk in iter(lambda: source.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def save_trace(path, hot_fraction: float, durations=STAGES['full'], seed: int = 17) -> str:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    spec = manifest(hot_fraction, seed)
    spec['durations'] = list(durations)
    with path.open('wb') as raw, gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0) as target:
        target.write((json.dumps(spec, sort_keys=True, separators=(',', ':')) + '\n').encode())
        for row in generate(spec, durations):
            target.write((json.dumps(row, separators=(',', ':')) + '\n').encode())
    return trace_hash(path)


def read_trace(path) -> tuple[dict, list]:
    with gzip.open(path, 'rt') as source:
        header = json.loads(next(source))
        rows = [tuple(json.loads(line)) for line in source]
    return header, rows
