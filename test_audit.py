import unittest


class IndependentAudit(unittest.TestCase):
    def events(self):
        return [dict(ts=i, replica='r0', **event) for i, event in enumerate([
            dict(event='load_start', lora='a', tier='cold'),
            dict(event='load_end', lora='a', tier='cold'),
            dict(event='batch_start', lora='a', size=1),
            dict(event='request_done', lora='a', id='one', customer='c'),
            dict(event='batch_end', lora='a', size=1),
            dict(event='evict', lora='a')])]

    def test_valid_event_stream_and_unique_completion(self):
        from audit import audit_events
        self.assertEqual(audit_events(self.events())['completed'], 1)
        events = self.events()
        events.insert(4, events[3])
        with self.assertRaises(AssertionError):
            audit_events(events)

    def test_illegal_loading_eviction_and_overlap_detected(self):
        from audit import audit_events
        for event in (dict(event='evict', lora='a'),
                      dict(event='batch_start', lora='a', size=1)):
            events = self.events()
            events.insert(1, dict(ts=.5, replica='r0', **event))
            with self.assertRaises(AssertionError):
                audit_events(events)

    def test_missing_completion_and_oversized_batch_detected(self):
        from audit import audit_events
        events = self.events()
        events[2]['size'] = 129
        with self.assertRaises(AssertionError):
            audit_events(events)
        events = self.events()
        events.pop(3)
        with self.assertRaises(AssertionError):
            audit_events(events)

    def test_conservation_requires_every_accepted_outcome_and_deadline(self):
        from audit import audit_requests
        rows = [(0, 'c', 'a'), (1, 'c', 'b')]
        good = {'0': dict(status='succeeded', end=2)}
        result = audit_requests(rows, {'0'}, good, 5)
        self.assertEqual(result['deadline_misses'], 0)
        self.assertEqual(result['rejected'], 1)
        self.assertEqual(audit_requests(rows, {'0'}, {}, 5)['outstanding'], 1)
        self.assertEqual(audit_requests(rows, {'0'}, {'0': dict(status='succeeded', end=6)}, 5)['deadline_misses'], 1)

    def test_engine_identities_are_cross_checked_against_accepted_requests(self):
        from audit import audit_events, crosscheck_engine
        rows = [(0, 'c', 'a')]
        events = self.events()
        for event in events:
            if event['event'] == 'request_done':
                event['id'] = '0'
        outcomes = {'0': dict(status='completed', start=2, end=4, output={'lora': 'a'})}
        crosscheck_engine(rows, {'0'}, outcomes, audit_events(events))
        for event in events:
            if event['event'] == 'request_done':
                event['customer'] = 'wrong-owner'
        with self.assertRaises(AssertionError):
            crosscheck_engine(rows, {'0'}, outcomes, audit_events(events))

    def test_equal_counts_do_not_hide_wrong_ids_or_premature_results(self):
        from audit import audit_events, crosscheck_engine
        events = self.events()
        outcomes = {'0': dict(status='completed', start=2, end=4, output={'lora': 'a'})}
        with self.assertRaises(AssertionError):
            crosscheck_engine([(0, 'c', 'a')], {'0'}, outcomes, audit_events(events))
        for event in events:
            if event['event'] == 'request_done':
                event['id'] = '0'
        outcomes['0']['end'] = 2.5
        with self.assertRaises(AssertionError):
            crosscheck_engine([(0, 'c', 'a')], {'0'}, outcomes, audit_events(events))
        outcomes['0']['end'] = 4
        outcomes['0']['output']['lora'] = 'wrong-adapter'
        with self.assertRaises(AssertionError):
            crosscheck_engine([(0, 'c', 'a')], {'0'}, outcomes, audit_events(events))

    def test_engine_time_is_clipped_to_windows_and_load_overlap_is_measured(self):
        from audit import audit_events, engine_time_summary
        events = [dict(replica=0, **event) for event in [
            dict(ts=0, event='batch_start', lora=None, size=1),
            dict(ts=.25, event='load_start', lora='a', tier='cold'),
            dict(ts=1.25, event='request_done', lora=None, id='0', customer='c'),
            dict(ts=1.25, event='batch_end', lora=None, size=1),
            dict(ts=2.25, event='load_end', lora='a', tier='cold')]]
        audited = audit_events(events)
        total = engine_time_summary(audited, 0, 3, replicas=2)
        self.assertEqual(total['compute_seconds'], 1.25)
        self.assertEqual(total['load_seconds'], 2)
        self.assertEqual(total['overlapped_load_seconds'], 1)
        self.assertEqual(total['exposed_load_seconds'], 1)
        self.assertEqual(total['compute_idle_seconds'], 4.75)
        first = engine_time_summary(audited, 0, 1, replicas=2)
        self.assertEqual(first['compute_seconds'], 1)
        self.assertEqual(first['load_seconds'], .75)
        self.assertEqual(first['overlapped_load_seconds'], .75)
        self.assertEqual(first['exposed_load_seconds'], 0)
