import unittest
import gzip
import json
import tempfile
from pathlib import Path


class ComparisonCards(unittest.TestCase):
    def test_joint_outcomes_and_no_acceptance_customer_explicit(self):
        from benchmark import comparison_card
        header = {'customers': ['a', 'b', 'z'], 'durations': [10, 10, 10]}
        rows = [(0, 'a', 'x'), (1, 'b', 'y'), (2, 'z', 'q')]
        outcomes = {'0': dict(status='completed', start=3, end=4, replica=0),
                    '1': dict(status='completed', start=12, end=13, replica=0)}
        events = [dict(event='batch_start', ts=3, size=1), dict(event='batch_start', ts=12, size=1)]
        card = comparison_card(header, rows, {'0', '1'}, {'2': 'tenant_limit'}, outcomes, events, 20)
        self.assertEqual(card['conservation']['accepted'], 2)
        self.assertEqual(card['customers']['z']['accepted'], 0)
        self.assertEqual(card['customers']['z']['rejection_rate'], 1)
        self.assertEqual(card['customers']['a']['max_wait'], 3)
        self.assertEqual(card['windows']['steady']['completed'], 1)
        self.assertEqual(card['windows']['burst']['completed'], 1)
        self.assertEqual(card['rejection_reasons'], {'tenant_limit': 1})
        self.assertEqual(card['customers_without_acceptance'], ['z'])

    def test_service_gap_resets_after_empty_backlog(self):
        from benchmark import customer_gaps
        rows = [(0, 'c', 'x'), (100, 'c', 'x')]
        outcomes = {'0': dict(start=5), '1': dict(start=103)}
        self.assertEqual(customer_gaps(rows, {'0', '1'}, outcomes)['c'], 5)
        rows = [(0, 'c', 'x'), (1, 'c', 'x'), (2, 'c', 'x')]
        outcomes = {'0': dict(start=5), '1': dict(start=9), '2': dict(start=17)}
        self.assertEqual(customer_gaps(rows, {'0', '1', '2'}, outcomes)['c'], 8)

    def test_unserved_customer_gap_extends_to_end_of_measurement(self):
        from benchmark import customer_gaps
        self.assertEqual(customer_gaps([(0, 'c', 'a')], {'0'}, {}, horizon=10)['c'], 10)

    def test_engine_start_is_fairness_ground_truth_and_drain_uses_measured_end(self):
        from benchmark import comparison_card
        header = {'customers': ['c'], 'durations': [10, 10, 10]}
        rows = [(0, 'c', 'a')]
        outcomes = {'0': dict(status='completed', start=2, engine_start=5, end=32)}
        card = comparison_card(header, rows, {'0'}, {}, outcomes, [], 20, measured_end=40)
        self.assertEqual(card['customers']['c']['max_wait'], 5)
        self.assertEqual(card['max_backlogged_service_gap'], 5)
        self.assertEqual(card['windows']['drain']['end'], 40)
        self.assertEqual(card['windows']['drain']['completed_goodput'], .1)
        self.assertEqual(card['windows']['drain']['deadline_goodput'], 0)

    def test_runner_base_work_crosschecks_engine_and_records_reproduction(self):
        from benchmark import run
        from scheduler import Config
        with tempfile.TemporaryDirectory() as directory:
            trace, output = Path(directory) / 'base.gz', Path(directory) / 'result.json'
            with gzip.open(trace, 'wt') as sink:
                sink.write(json.dumps({'customers': ['c'], 'durations': [1, 1, 1], 'seed': 17})+'\n')
                sink.write(json.dumps([0, 'c', None])+'\n')
            card = run(trace, Config(max_batch_wait=0), output)
            self.assertEqual(card['conservation']['completed'], 1)
            self.assertEqual(card['conservation']['deadline_misses'], 0)
            self.assertEqual(card['engine_audit']['compute_seconds'], 1.25)
            self.assertIn('--config '+str(output), card['command'])
            with gzip.open(output.with_suffix('.requests.jsonl.gz'), 'rt') as source:
                outcome = json.loads(next(source))[3]
            self.assertEqual(outcome['engine_start'], 0)

    def test_replay_cohort_exclusions_are_not_reported_as_rejections(self):
        from benchmark import comparison_card
        header = {'customers': ['a', 'b'], 'durations': [1, 1, 1]}
        rows = [(0, 'a', None), (0, 'b', None)]
        outcomes = {'1': dict(status='completed', start=0, end=1.25)}
        card = comparison_card(header, rows, {'1'}, {}, outcomes, [], 20, offered_ids={'1'})
        self.assertEqual(card['conservation']['offered'], 1)
        self.assertEqual(card['conservation']['rejected'], 0)
        self.assertEqual(card['rejection_rate'], 0)
        self.assertEqual(card['customers']['a']['offered'], 0)
        self.assertEqual(card['customers']['a']['rejected'], 0)

    def test_replay_requires_matching_trace_provenance_and_valid_ids(self):
        from benchmark import run
        from scheduler import Config
        from workload import trace_hash
        with tempfile.TemporaryDirectory() as directory:
            trace, source_card = Path(directory) / 'trace.gz', Path(directory) / 'source.json'
            replay = source_card.with_suffix('.requests.jsonl.gz')
            with gzip.open(trace, 'wt') as sink:
                sink.write(json.dumps({'customers': ['c'], 'durations': [1, 1, 1], 'seed': 17})+'\n')
                sink.write(json.dumps([0, 'c', None])+'\n')
            with gzip.open(replay, 'wt') as sink:
                sink.write(json.dumps([0, True, None, None])+'\n')
            source_card.write_text(json.dumps({'trace_hash': 'another-trace'}))
            with self.assertRaisesRegex(ValueError, 'trace'):
                run(trace, Config(max_batch_wait=0), Path(directory)/'result.json', replay=replay)
            source_card.write_text(json.dumps({'trace_hash': trace_hash(trace)}))
            with gzip.open(replay, 'wt') as sink:
                sink.write(json.dumps([5, True, None, None])+'\n')
            with self.assertRaisesRegex(ValueError, 'bounds'):
                run(trace, Config(max_batch_wait=0), Path(directory)/'result.json', replay=replay)

    def test_offered_requests_partition_into_accepted_and_explicit_rejections(self):
        from benchmark import comparison_card
        header = {'customers': ['c'], 'durations': [1, 1, 1]}
        with self.assertRaises(AssertionError):
            comparison_card(header, [(0, 'c', None)], set(), {}, {}, [], 20)

    def test_saved_raw_results_can_be_independently_reaudited(self):
        import gzip
        import json
        import tempfile
        from pathlib import Path
        from benchmark import run, audit_card
        from scheduler import Config
        with tempfile.TemporaryDirectory() as tmp:
            trace = Path(tmp) / 'trace.gz'
            with gzip.open(trace, 'wt') as sink:
                sink.write(json.dumps({'seed':17,'customers':['c'],'durations':[1,1,1]})+'\n')
                sink.write(json.dumps([0,'c',None])+'\n')
            card = Path(tmp) / 'result.json'
            run(trace,Config(replicas=1,max_batch_wait=0),card)
            self.assertEqual(audit_card(card)['completed'],1)
            events = card.with_suffix('.events.jsonl.gz')
            with gzip.open(events,'rt') as source:
                rows = [json.loads(line) for line in source]
            for row in rows:
                if row['event']=='request_done':
                    row['id']='invented'
            with gzip.open(events,'wt') as sink:
                sink.writelines(json.dumps(row)+'\n' for row in rows)
            with self.assertRaises(AssertionError):
                audit_card(card)

    def test_post_run_audit_cost_does_not_extend_measurement_horizon(self):
        import gzip
        import json
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        from benchmark import run
        from backends import VirtualBackend
        from scheduler import Config
        class SlowInspection(VirtualBackend):
            def event_log(self):
                events=super().event_log()
                self.now+=100
                return events
        with tempfile.TemporaryDirectory() as tmp:
            trace=Path(tmp)/'trace.gz'
            with gzip.open(trace,'wt') as sink:
                sink.write(json.dumps({'seed':17,'customers':['c'],'durations':[1,1,1]})+'\n')
                sink.write(json.dumps([0,'c',None])+'\n')
            with patch('backends.VirtualBackend',SlowInspection):
                card=run(trace,Config(replicas=1,max_batch_wait=0),Path(tmp)/'result.json',raw=False)
            self.assertEqual(card['windows']['drain']['end'],3)
            self.assertEqual(card['engine_audit']['compute_idle_seconds'],1.75)

    def test_due_arrival_backlog_cannot_starve_completion_publication(self):
        import gzip
        import json
        import tempfile
        from pathlib import Path
        from unittest.mock import patch
        from benchmark import run
        from scheduler import Config, Scheduler
        class CostlyAdmission(Scheduler):
            def submit(self, request):
                decision=super().submit(request)
                self.backend.advance(self.backend.now+.02)
                return decision
        with tempfile.TemporaryDirectory() as tmp:
            trace=Path(tmp)/'trace.gz'
            with gzip.open(trace,'wt') as sink:
                sink.write(json.dumps({'seed':17,'customers':['c'],'durations':[1,1,1]})+'\n')
                sink.writelines(json.dumps([0,'c',None])+'\n' for _ in range(500))
            config=Config(replicas=4,max_batch_wait=0,starvation_threshold=1,slo=5,
                          controller_margin=.25,response_margin=.25)
            with patch('scheduler.Scheduler',CostlyAdmission):
                card=run(trace,config,Path(tmp)/'result.json',raw=False)
            self.assertGreater(card['conservation']['completed'],0)
            self.assertEqual(card['conservation']['deadline_misses'],0)
            self.assertLessEqual(card['control_timing']['max_gap'],.081)

    def test_offline_replay_audit_checks_source_cohort_identities(self):
        from benchmark import run, audit_card
        from scheduler import Config
        from workload import trace_hash
        with tempfile.TemporaryDirectory() as tmp:
            trace=Path(tmp)/'trace.gz'
            with gzip.open(trace,'wt') as sink:
                sink.write(json.dumps({'seed':17,'customers':['c'],'durations':[1,1,1]})+'\n')
                sink.writelines(json.dumps([0,'c',None])+'\n' for _ in range(2))
            source_card=Path(tmp)/'source.json'
            source_card.write_text(json.dumps({'trace_hash':trace_hash(trace)}))
            cohort=source_card.with_suffix('.requests.jsonl.gz')
            with gzip.open(cohort,'wt') as sink:
                sink.write(json.dumps([0,True,None,None])+'\n')
                sink.write(json.dumps([1,False,'excluded',None])+'\n')
            target=Path(tmp)/'replay.json'
            run(trace,Config(max_batch_wait=0),target,replay=cohort)
            self.assertEqual(audit_card(target)['completed'],1)
            # Keep the same count, valid engine identity and timing, but replace
            # cohort request 0 with a different valid request from the trace.
            with gzip.open(target.with_suffix('.requests.jsonl.gz'),'rt') as stream:
                raw=[json.loads(line) for line in stream]
            raw[1][1],raw[1][3]=raw[0][1],raw[0][3]
            raw[0][1],raw[0][3]=False,None
            with gzip.open(target.with_suffix('.requests.jsonl.gz'),'wt') as sink:
                sink.writelines(json.dumps(row)+'\n' for row in raw)
            with gzip.open(target.with_suffix('.events.jsonl.gz'),'rt') as stream:
                events=[json.loads(line) for line in stream]
            for event in events:
                if event['event']=='request_done': event['id']='1'
            with gzip.open(target.with_suffix('.events.jsonl.gz'),'wt') as sink:
                sink.writelines(json.dumps(event)+'\n' for event in events)
            with self.assertRaisesRegex(AssertionError,'cohort'):
                audit_card(target)

    def test_sparse_replay_skips_excluded_arrivals_but_preserves_engine_and_batch_timers(self):
        from unittest.mock import patch
        from backends import VirtualBackend
        from scheduler import Config, Scheduler
        from benchmark import run, audit_card
        from workload import trace_hash
        work = {'ticks': 0, 'advances': []}
        class CountingBackend(VirtualBackend):
            def advance(self,to):
                work['advances'].append(to)
                return super().advance(to)
        class CountingScheduler(Scheduler):
            def tick(self,*args,**kwargs):
                work['ticks'] += 1
                return super().tick(*args,**kwargs)
        with tempfile.TemporaryDirectory() as tmp:
            trace = Path(tmp)/'trace.gz'
            cohort = {100,2000}
            with gzip.open(trace,'wt') as sink:
                sink.write(json.dumps({'seed':17,'customers':['a','b','excluded'],'durations':[100,100,100]})+'\n')
                for index in range(3000):
                    customer,lora = (('a','cold-a') if index == 100 else ('b','cold-b') if index == 2000 else ('excluded',None))
                    sink.write(json.dumps([index/10,customer,lora])+'\n')
            source_card = Path(tmp)/'source.json'
            source_card.write_text(json.dumps({'trace_hash':trace_hash(trace)}))
            replay = source_card.with_suffix('.requests.jsonl.gz')
            with gzip.open(replay,'wt') as sink:
                sink.writelines(json.dumps([index,index in cohort,None,None])+'\n' for index in range(3000))
            target = Path(tmp)/'replay.json'
            config = Config(replicas=1,max_batch_wait=5,eligibility='pending')
            with patch('backends.VirtualBackend',CountingBackend), patch('scheduler.Scheduler',CountingScheduler):
                card = run(trace,config,target,replay=replay)
            self.assertEqual(card['conservation']['offered'],2)
            self.assertEqual(card['conservation']['completed'],2)
            self.assertEqual(card['excluded_from_replay'],2998)
            self.assertEqual(audit_card(target),card['conservation'])
            with gzip.open(target.with_suffix('.requests.jsonl.gz'),'rt') as source:
                evidence = [json.loads(line) for line in source]
            self.assertEqual(len(evidence),3000)
            self.assertEqual({row[0] for row in evidence if row[1]},cohort)
            self.assertAlmostEqual(evidence[100][3]['engine_start'],12)
            self.assertAlmostEqual(evidence[100][3]['end'],13.25)
            self.assertAlmostEqual(evidence[2000][3]['engine_start'],202)
            self.assertAlmostEqual(evidence[2000][3]['end'],203.25)
            self.assertLess(work['ticks'],30,'excluded trace rows must not drive scheduler ticks')
            self.assertLess(len(work['advances']),30,'excluded trace rows must not drive virtual wakeups')

    def test_mixed_base_and_owned_lora_trace_writes_json_and_independently_audits(self):
        from benchmark import run, audit_card
        from scheduler import Config
        with tempfile.TemporaryDirectory() as tmp:
            trace,target = Path(tmp)/'mixed.gz',Path(tmp)/'mixed.json'
            with gzip.open(trace,'wt') as sink:
                sink.write(json.dumps({'seed':17,'customers':['c'],'durations':[1,1,2]})+'\n')
                sink.write(json.dumps([0,'c',None])+'\n')
                sink.write(json.dumps([.1,'c','c/support@v1'])+'\n')
            card = run(trace,Config(max_batch_wait=0),target)
            saved = json.loads(target.read_text())
            self.assertEqual(saved['conservation']['completed'],2)
            self.assertEqual(set(saved['adapter_max_wait']),{'base','c/support@v1'})
            self.assertEqual(audit_card(target),card['conservation'])
