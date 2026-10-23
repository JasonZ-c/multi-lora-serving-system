"""Deterministic scheduler and original-engine adapter contract checks."""
import dataclasses
import queue
import threading
import time
import unittest
from unittest.mock import patch

from backends import VirtualBackend, ThreadedBackend
from scheduler import Config, Request, Scheduler


def req(i, customer='c', lora=None, at=0.0, deadline=900.0):
    return Request(str(i), customer, lora, '/weights/' + lora if lora else None,
                   'hello', at, deadline)


def drain(s, b, limit=10000):
    for _ in range(limit):
        s.tick()
        if not s.pending:
            return
        wake = s.next_wakeup()
        if wake is None:
            raise AssertionError('pending accepted work has no wakeup')
        b.advance(max(b.now, wake))
    raise AssertionError('scheduler did not drain')


class VirtualContract(unittest.TestCase):
    def test_overlap_capacity_and_warm_reload(self):
        b = VirtualBackend(replicas=1, slots=1)
        b.start_load(0, 'a', '/a', 'load-a')
        b.advance(2)
        self.assertTrue(b.poll()[0]['ok'])
        b.start_batch(0, [{'id':'1', 'lora':'a'}], 'batch-a')
        with self.assertRaises(Exception):
            b.evict(0, 'a')
        b.advance(3.25)
        b.poll()
        b.evict(0, 'a')
        b.start_load(0, 'b', '/b', 'load-b')
        with self.assertRaises(Exception):
            b.start_load(0, 'a', '/a', 'illegal')
        b.advance(5.25)
        b.poll()
        b.evict(0, 'b')
        b.start_load(0, 'a', '/a', 'warm-a')
        b.start_batch(0, [{'id':'2', 'lora':None}], 'base')
        b.advance(5.45)
        self.assertEqual(b.poll()[0]['token'], 'warm-a')
        self.assertEqual(b.next_event(), 6.5)

    def test_batch_shape_is_enforced(self):
        b = VirtualBackend(1)
        for inputs in ([], [{'lora':None}] * 129, [{'lora':'a'}, {'lora':'b'}]):
            with self.assertRaises(Exception):
                b.start_batch(0, inputs, 'bad')

    def test_near_event_advance_never_moves_clock_backwards(self):
        b=VirtualBackend(1)
        b.start_batch(0,[{'id':'first'}],1)
        b.advance(1.25-5e-11)
        # Processing an event within a numeric tolerance must not reset the
        # clock below the event before another operation is started.
        b.start_load(0,'a','/a',2)
        stamps=[e['ts'] for e in b.events]
        self.assertEqual(stamps,sorted(stamps))


class SchedulerContract(unittest.TestCase):
    def test_default_ready_gate_does_not_treat_a_small_ticket_as_a_ready_customer(self):
        self.assertEqual(Config().eligibility, 'ready')
        b=VirtualBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=30),b)
        s.submit(req('small',customer='a'))
        for i in range(128):
            s.submit(req('full'+str(i),customer='b'))
        s.customers['a'].virtual_service=0
        s.customers['b'].virtual_service=10
        s.tick()
        self.assertEqual(s.request_status('small')['status'],'queued')
        self.assertIsNone(s.customers['a'].last_grant)
        self.assertEqual(s.request_status('full0')['status'],'running')
        self.assertEqual(s.customers['b'].last_grant,0)

    def test_idle_replica_immediately_serves_a_single_young_request(self):
        for policy in ('fifo','fair','locality'):
            for lora in (None,'cold'):
                with self.subTest(policy=policy,lora=lora):
                    b=VirtualBackend(1)
                    s=Scheduler(Config(replicas=1,policy=policy,max_batch_wait=120,eligibility='pending'),b)
                    self.assertTrue(s.submit(req('single',lora=lora))[0])
                    s.tick()
                    self.assertEqual(s.customers['c'].last_grant,0)
                    self.assertIsNotNone(s.replicas[0].foreground)
                    drain(s,b)
                    self.assertEqual(s.completed['single']['end'],1.25 if lora is None else 3.25)

    def test_young_partial_customer_participates_in_virtual_service_selection(self):
        b=VirtualBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=120,eligibility='pending'),b)
        s.submit(req('young',customer='a'))
        for i in range(128):
            s.submit(req('full'+str(i),customer='b'))
        s.customers['a'].virtual_service=0
        s.customers['b'].virtual_service=10
        s.tick()
        self.assertEqual(s.request_status('young')['status'],'running')
        self.assertIsNone(s.customers['b'].last_grant)

    def test_young_partial_choice_still_yields_to_edf_safety(self):
        b=VirtualBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=120,eligibility='pending'),b)
        self.assertTrue(s.submit(req('urgent',customer='z',lora='cold',deadline=3.3))[0])
        self.assertTrue(s.submit(req('young',customer='a',deadline=100))[0])
        s.tick()
        self.assertEqual(s.customers['z'].last_grant,0)
        self.assertGreater(s.metrics['edf_overrides'],0)
        drain(s,b)
        self.assertLessEqual(s.completed['urgent']['end'],3.3)
        self.assertEqual(s.completed['young']['start'],3.25)

    def test_batch_wait_and_default_customer_gap_trigger_are_independent(self):
        self.assertEqual(Config().starvation_threshold,60)
        self.assertTrue(Config().service_gap_guard)
        try:
            config=Config(max_batch_wait=120,starvation_threshold=60)
        except ValueError as error:
            self.fail(f'Batch wait and customer gap must be independent: {error}')
        self.assertEqual(config.max_batch_wait,120)
        self.assertEqual(config.starvation_threshold,60)
        for settings in ({'max_batch_wait':-1},{'max_batch_wait':900},
                         {'starvation_threshold':-1},{'starvation_threshold':900},
                         {'eligibility':'small'}):
            with self.subTest(settings=settings),self.assertRaises(ValueError):
                Config(**settings)

    def test_customer_gap_trigger_includes_young_partial_work(self):
        b=VirtualBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=120,starvation_threshold=60,eligibility='pending'),b)
        s.submit(req('partial',customer='a'))
        b.advance(60)
        for i in range(128):
            s.submit(req('full'+str(i),customer='b',at=60,deadline=960))
        s.customers['a'].virtual_service=1000
        s.customers['b'].virtual_service=0
        s.tick()
        self.assertEqual(s.request_status('partial')['status'],'running')
        self.assertEqual(s.customers['a'].last_grant,60)
        self.assertEqual(s.metrics['service_gap_overrides'],1)
        b.advance(61.25)
        s.tick()
        self.assertEqual(s.metrics['timing_bound_violations'],0)

    def test_blocked_overdue_customer_does_not_block_a_legal_young_partial(self):
        b=VirtualBackend(2,slots=1)
        s=Scheduler(Config(replicas=2,max_batch_wait=120,eligibility='pending'),b)
        s.submit(req('blocked',customer='a',lora='needed'))
        self.assertEqual(s.tickets[s._request_ticket['blocked']].replica,0)
        b.advance(60)
        end=b.start_load(0,'other','/weights/other','blocking')
        s.replicas[0].loading=dict(lora='other',path='/weights/other',
                                  end=end,token='blocking',prefetch=True)
        s.submit(req('available',customer='b',at=60))
        s.tick()
        self.assertEqual(s.request_status('blocked')['status'],'queued')
        self.assertEqual(s.request_status('available')['status'],'running')
        self.assertIsNone(s.customers['a'].last_grant)
        self.assertEqual(s.customers['b'].last_grant,60)
        drain(s,b)
        self.assertEqual(s.completed['blocked']['status'],'completed')

    def test_removed_experimental_policy_options_are_rejected(self):
        with self.assertRaises(TypeError):
            Config(score='marginal')
        with self.assertRaises(TypeError):
            Config(slru=True)
        with self.assertRaises(TypeError):
            Config(opportunistic=False)

    def test_eviction_uses_lru_actual_use_without_second_hit_segment(self):
        b=VirtualBackend(1,slots=2)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,policy='locality',
                           demand_eviction=False),b)
        for i,lora in enumerate(('a','a','b')):
            s.submit(req(i,lora=lora,at=b.now,deadline=b.now+100))
            drain(s,b)
        s.submit(req('new',lora='c',at=b.now,deadline=b.now+100))
        s.tick()
        self.assertEqual([e['lora'] for e in b.events if e['event']=='evict'],['a'])

    def make(self, **kwargs):
        count = kwargs.pop('replicas', 1)
        b = VirtualBackend(count)
        return Scheduler(Config(replicas=count, max_batch_wait=0, **kwargs), b), b

    def test_homogeneous_immutable_tickets_and_batch_limit(self):
        s, b = self.make()
        for i in range(129):
            self.assertTrue(s.submit(req(i, lora='a'))[0])
        self.assertEqual(sorted(len(t.requests) for t in s.tickets.values()), [1,128])
        ticket = next(iter(s.tickets.values()))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            ticket.replica = 4
        drain(s,b)
        self.assertEqual(len(s.completed),129)
        sizes = [e['size'] for e in b.events if e['event']=='batch_start']
        self.assertEqual(sizes,[128,1])
        self.assertTrue(all(o['status']=='completed' for o in s.completed.values()))

    def test_full_or_timeout_is_a_preference_within_the_selected_customer(self):
        for age in (0,10):
            with self.subTest(age=age):
                b=VirtualBackend(1)
                s=Scheduler(Config(replicas=1,max_batch_wait=10,eligibility='pending'),b)
                s.submit(req('partial'))
                for i in range(128):
                    s.submit(req('full'+str(i),lora='cold'))
                b.advance(age)
                s.tick()
                first=next(e for e in s.events if e['event']=='grant')
                self.assertEqual(first['lora'],'cold' if age==0 else None)
                drain(s,b)
                if age==0:
                    self.assertEqual(s.completed['partial']['start'],3.25)

    def test_fifo_only_coalesces_contiguous_arrivals(self):
        s,b = self.make(policy='fifo')
        for i,lora in enumerate(('a','b','a')):
            s.submit(req(i,lora=lora))
        self.assertEqual(len(s.tickets),3)
        drain(s,b)
        self.assertEqual([e['lora'] for e in b.events if e['event']=='batch_start'],['a','b','a'])

    def test_fair_reserves_service_before_four_grants(self):
        b=VirtualBackend(4)
        s=Scheduler(Config(replicas=4,max_batch_wait=120,eligibility='pending'),b)
        for customer in ('a','b','c','d'):
            for i in range(4):
                s.submit(req(customer+str(i),customer,lora=customer+str(i)))
        s.tick()
        grants=[e for e in s.events if e['event']=='grant']
        self.assertEqual({e['customer'] for e in grants},{'a','b','c','d'})
        self.assertEqual(len(grants),4)
        self.assertTrue(all(s.customers[c].virtual_service>0 for c in ('a','b','c','d')))

    def test_staggered_deadline_edf_and_early_flush(self):
        b=VirtualBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=30,starvation_threshold=60),b)
        self.assertTrue(s.submit(req('urgent',deadline=1.5))[0])
        self.assertTrue(s.submit(req('later',customer='other',deadline=10))[0])
        drain(s,b)
        self.assertLessEqual(s.completed['urgent']['end'],1.5)
        self.assertLessEqual(s.completed['later']['end'],10)

    def test_admission_protects_prior_commitment(self):
        s,b=self.make()
        self.assertTrue(s.submit(req('first',lora='a',deadline=3.3))[0])
        accepted,reason=s.submit(req('second',lora='b',deadline=3.3))
        self.assertFalse(accepted)
        self.assertIn(reason,('deadline_infeasible','conservative_capacity'))
        drain(s,b)
        self.assertLessEqual(s.completed['first']['end'],3.3)

    def test_cold_deadline_survives_same_customer_hot_service(self):
        s,b=self.make(policy='locality')
        s.submit(req('cold',lora='cold',deadline=4.6))
        for i in range(256):
            s.submit(req('hot'+str(i),lora=None,deadline=100))
        drain(s,b)
        self.assertLessEqual(s.completed['cold']['end'],4.6)
        self.assertGreater(s.metrics['edf_overrides'],0)

    def test_actual_grant_resets_gap_but_prefetch_does_not(self):
        s,b=self.make(policy='locality',prefetch=True)
        s.submit(req('a',customer='a',lora='a'))
        s.submit(req('b',customer='a',lora='b'))
        s.tick()
        self.assertEqual(s.customers['a'].last_grant,0)
        b.advance(2)
        s.tick()
        self.assertEqual(s.customers['a'].last_grant,0)
        b.advance(3.25)
        s.tick()
        self.assertEqual(s.customers['a'].last_grant,3.25)

    def test_migration_rechecks_both_ledgers(self):
        s,b=self.make(replicas=2)
        s.submit(req('a',lora='a',deadline=3.3))
        s.submit(req('b',lora='b',deadline=3.3))
        tickets=sorted(s.tickets.values(),key=lambda t:t.id)
        self.assertNotEqual(tickets[0].replica,tickets[1].replica)
        self.assertFalse(s.migrate(tickets[1].id,tickets[0].replica))
        drain(s,b)
        self.assertEqual(len(s.completed),2)

    def test_retries_are_fenced_and_keep_original_deadline(self):
        class FailOnce(VirtualBackend):
            failed=False
            def start_batch(self, replica, inputs, token):
                if not self.failed:
                    self.failed=True
                    self.inject_completion({'kind':'batch','replica':replica,'token':token,
                                            'ok':False,'error':'transient','at':self.now})
                else:
                    super().start_batch(replica,inputs,token)
        b=FailOnce(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,max_retries=1),b)
        s.submit(req('a',deadline=10))
        s.tick(); old=s.replicas[0].token
        s.tick()
        b.inject_completion({'kind':'batch','replica':0,'token':old,'ok':True,
                             'result':[{'id':'a','output':'stale'}],'at':b.now})
        drain(s,b)
        self.assertEqual(s.completed['a']['attempt'],2)
        self.assertNotEqual(s.completed['a']['output']['output'],'stale')
        self.assertLessEqual(s.completed['a']['end'],10)

    def test_accepted_work_replay_can_disable_admission_guard(self):
        s,b=self.make(admission_guard=False)
        self.assertTrue(s.submit(req('a',deadline=.1))[0])
        drain(s,b)
        self.assertGreater(s.completed['a']['end'],.1)
        self.assertEqual(s.metrics['deadline_violations'],1)

    def test_tenant_cap_is_separate_rejection(self):
        s,b=self.make(tenant_ticket_cap=1)
        self.assertTrue(s.submit(req('a',lora='a'))[0])
        self.assertEqual(s.submit(req('b',lora='b')),(False,'tenant_limit'))

    def test_result_availability_includes_delayed_controller_poll(self):
        s,b=self.make()
        s.submit(req('a',deadline=1.5))
        s.tick()
        b.advance(2)
        s.tick()
        self.assertEqual(s.completed['a']['end'],2)
        self.assertEqual(s.completed['a']['engine_end'],1.25)
        self.assertEqual(s.metrics['deadline_violations'],1)

    def test_idle_replica_starts_young_cold_work_before_edf_slack_exhaustion(self):
        b=VirtualBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=10,eligibility='pending'),b)
        s.submit(req('urgent',lora='a',deadline=4))
        s.tick()
        self.assertEqual(s.customers['c'].last_grant,0)
        self.assertEqual(s.next_wakeup(),2)
        drain(s,b)
        self.assertLessEqual(s.completed['urgent']['end'],4)

    def test_demand_eviction_is_soft_and_preserves_pending_commitments(self):
        b=VirtualBackend(1,slots=2)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,policy='locality'),b)
        for i,lora in enumerate(('a','b','c','a','b')):
            s.submit(req(i,lora=lora))
            drain(s,b)
        self.assertEqual(len(s.completed),5)
        self.assertGreater(s.metrics['evictions'],0)
        self.assertTrue(any(e['event']=='load_start' and e['tier']=='warm' for e in b.events))
        self.assertTrue(all(o['status']=='completed' for o in s.completed.values()))

    def test_prefetch_hides_load_but_never_resets_other_customer_gap(self):
        s,b=self.make(policy='locality',prefetch=True)
        s.submit(req('a',customer='a',lora=None))
        s.submit(req('b',customer='b',lora='b'))
        s.tick()
        self.assertEqual(s.customers['b'].last_grant,None)
        load=next(e for e in b.events if e['event']=='load_start')
        batch=next(e for e in b.events if e['event']=='batch_start')
        self.assertEqual(load['ts'],batch['ts'])
        self.assertLessEqual(len(s.replicas[0].pins),2)
        drain(s,b)
        self.assertEqual(s.completed['b']['end'],3.25)

    def test_prefetch_rejected_when_blocking_loses_edf_fallback(self):
        s,b=self.make(policy='locality',prefetch=True)
        s.submit(req('base',customer='a',deadline=1.3))
        s.submit(req('cold',customer='b',lora='cold',deadline=100))
        s.submit(req('other',customer='c',deadline=2.6))
        s.tick()
        self.assertEqual(s.metrics['prefetches'],0)
        # A cold loader lasting until 2 would conservatively postpone the
        # other accepted base batch past its 2.6 deadline.
        drain(s,b)
        self.assertTrue(all(o['end']<=o['deadline'] for o in s.completed.values()))

    def test_cancel_replaces_ticket_without_leaking_commitments(self):
        s,b=self.make()
        s.submit(req('a'))
        s.submit(req('b'))
        self.assertTrue(s.cancel('a'))
        self.assertEqual(s.pending,1)
        drain(s,b)
        self.assertEqual(set(s.completed),{'b'})

    def test_idle_customer_rejoins_at_active_virtual_minimum(self):
        s,b=self.make()
        s.submit(req('first',customer='a'))
        drain(s,b)
        s.submit(req('new',customer='b',at=b.now,deadline=b.now+100))
        s.tick()
        s.submit(req('return',customer='a',at=b.now,deadline=b.now+100))
        self.assertEqual(s.customers['a'].virtual_service,
                         s.customers['b'].virtual_service)

    def test_blocked_edf_worker_does_not_idle_another_available_worker(self):
        s,b=self.make(replicas=2,policy='locality',prefetch=True)
        for i in range(129):
            s.submit(req('running'+str(i),customer='a'))
        s.tick()
        s.submit(req('prefetch',customer='a',lora='b',deadline=100))
        tid=s._request_ticket['prefetch']
        self.assertTrue(s.migrate(tid,0))
        s.submit(req('edf',customer='a',lora='c',deadline=5.3))
        tid=s._request_ticket['edf']
        self.assertTrue(s.migrate(tid,0))
        for i in range(128):
            s.submit(req('tempting'+str(i),customer='a',deadline=100))
        tid=s._request_ticket['tempting0']
        self.assertTrue(s.migrate(tid,0))
        self.assertTrue(s.submit(req('other',customer='z',deadline=2.6))[0])
        s.tick()
        self.assertEqual(s.replicas[0].loading['lora'],'b')
        b.advance(1.25)
        s.tick()
        self.assertIsNotNone(s.replicas[1].foreground)
        drain(s,b)
        self.assertEqual(s.metrics['deadline_violations'],0)

    def test_backlog_gap_begins_when_new_queue_arrives_during_compute(self):
        s,b=self.make()
        s.submit(req('running'))
        s.tick()
        b.advance(1)
        s.submit(req('new',at=1))
        self.assertEqual(s.customers['c'].backlog_since,1)
        self.assertEqual(s._gap(s.customers['c']),0)

    def test_join_after_controller_timing_loss_is_rejected(self):
        b=VirtualBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=30),b)
        s.submit(req('old',deadline=10))
        b.advance(10)
        accepted,reason=s.submit(req('new',at=10,deadline=10))
        self.assertFalse(accepted)
        self.assertEqual(reason,'deadline_infeasible')

    def test_phase_and_customer_guard_metrics(self):
        b=VirtualBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=1,starvation_threshold=1,
                           policy='locality'),b)
        s.submit(req('a',lora='a'))
        drain(s,b)
        self.assertEqual(s.metrics['coalescing_idle_seconds'],0)
        self.assertEqual(s.metrics['exposed_load_seconds'],2)
        # The ready gate holds this singleton until max_batch_wait, which equals
        # the gap threshold, so the customer is overdue at the grant.
        self.assertEqual(s.metrics['service_gap_overrides'],1)

    def test_pair_score_keeps_foreground_load_reservation_until_compute(self):
        s,b=self.make(policy='locality')
        s.submit(req('a',lora='a'))
        s.submit(req('b',lora='b'))
        s.tick()
        b.advance(1)
        ticket=s.tickets[s._request_ticket['b']]
        self.assertEqual(s._pair_cost(ticket,s.replicas[0]),6.5)
        # Admission uses the *remaining* 2.25 load/compute sequence instead.
        self.assertEqual(s._foreground_remaining(s.replicas[0]),2.25)

    def test_replication_moves_only_full_batch_when_saved_wait_justifies_load(self):
        for size in (1,128):
            with self.subTest(size=size):
                s,b=self.make(replicas=2,policy='locality',replication=True)
                s.submit(req('blocking',lora='blocking'))
                s.tick()
                for i in range(size):
                    s.submit(req('hot'+str(i),lora='hot'))
                tid=s._request_ticket['hot0']
                self.assertTrue(s.migrate(tid,0))
                s.tick()
                self.assertEqual(s.metrics['replications'],int(size==128))
                self.assertEqual(s.tickets[tid].replica,int(size==128))
                drain(s,b)
                self.assertEqual(s.metrics['deadline_violations'],0)

    def test_demand_can_evict_when_every_legal_victim_has_queued_demand(self):
        b=VirtualBackend(1,slots=2)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,policy='locality'),b)
        for lora in ('a','b'):
            s.submit(req('warm'+lora,lora=lora,at=b.now,deadline=b.now+100))
            drain(s,b)
        for lora in ('a','b'):
            s.submit(req('queued'+lora,lora=lora,at=b.now,deadline=b.now+100))
        self.assertTrue(s.submit(req('cold',lora='c',at=b.now,deadline=b.now+3.3))[0])
        s.tick()
        self.assertEqual(s.replicas[0].loading['lora'],'c')
        drain(s,b)
        self.assertEqual(s.metrics['deadline_violations'],0)
        self.assertGreater(s.metrics['evictions'],0)

    def test_terminal_accepted_failure_counts_as_violation_even_before_deadline(self):
        class FailedBackend(VirtualBackend):
            def start_batch(self,replica,inputs,token):
                self.inject_completion(dict(kind='batch',replica=replica,
                    token=token,ok=False,error='permanent',at=self.now))
                return self.now
        b=FailedBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,max_retries=0),b)
        s.submit(req('a'))
        s.tick();s.tick()
        self.assertEqual(s.completed['a']['status'],'failed')
        self.assertEqual(s.metrics['deadline_violations'],1)
        self.assertEqual(s.pending,0)

    def test_load_transition_does_not_replenish_committed_controller_margin(self):
        b=VirtualBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,controller_margin=.25),b)
        s.submit(req('cold',lora='cold'))
        s.tick()
        self.assertAlmostEqual(s._foreground_remaining(s.replicas[0]),3.5)
        b.advance(2.2)
        s.tick()
        self.assertAlmostEqual(s._foreground_remaining(s.replicas[0]),1.3)

    def test_admission_fails_closed_after_active_compute_timing_bound_is_lost(self):
        class DelayedBackend(VirtualBackend):
            delayed=False
            def _enqueue(self,at,kind,replica,token,data):
                if kind=='batch' and not self.delayed:
                    self.delayed=True
                    super()._enqueue(at+5,kind,replica,token,data)
                    return at
                return super()._enqueue(at,kind,replica,token,data)
        b=DelayedBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,controller_margin=.25),b)
        s.submit(req('running'))
        s.tick()
        s.submit(req('waiting'))
        s.submit(req('certifies_join'))
        b.advance(2)
        self.assertEqual(s.submit(req('after_bound',at=2)),(False,'no_healthy_replica'))
        self.assertEqual(s.metrics['timing_bound_violations'],1)
        b.advance(6.25)
        s.tick()
        self.assertTrue(s.submit(req('after_recovery',at=b.now))[0])
        drain(s,b)

    def test_admission_checks_active_loader_before_its_sequence_end(self):
        class DelayedLoad(VirtualBackend):
            def _enqueue(self,at,kind,replica,token,data):
                if kind=='load':
                    super()._enqueue(at+1,kind,replica,token,data)
                    return at
                return super()._enqueue(at,kind,replica,token,data)
        b=DelayedLoad(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,controller_margin=.25),b)
        s.submit(req('cold',lora='cold'))
        s.tick()
        b.advance(2.5)
        self.assertEqual(s.submit(req('new',at=2.5,deadline=5)),
                         (False,'no_healthy_replica'))

    def test_grant_on_prefetch_uses_one_remaining_sequence_allowance(self):
        b=VirtualBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,controller_margin=.25,
                           policy='locality',prefetch=True),b)
        s.submit(req('base'))
        s.submit(req('cold',lora='cold'))
        s.tick()
        b.advance(1.25)
        s.tick()
        self.assertAlmostEqual(s.replicas[0].foreground_bound,3.5)

    def test_unprotected_replay_keeps_coalescing_after_timing_bound_loss(self):
        class DelayedBackend(VirtualBackend):
            def _enqueue(self,at,kind,replica,token,data):
                super()._enqueue(at+5,kind,replica,token,data)
                return at
        b=DelayedBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,admission_guard=False),b)
        s.submit(req('running'))
        s.tick()
        s.submit(req('waiting'))
        tid=s._request_ticket['waiting']
        b.advance(2)
        self.assertTrue(s.submit(req('joins',at=2))[0])
        self.assertEqual(s._request_ticket['joins'],tid)

    def test_controller_margin_covers_both_phases_without_deadline_extension(self):
        b=VirtualBackend(1)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,controller_margin=.25),b)
        self.assertTrue(s.submit(req('cold',lora='cold',deadline=3.5))[0])
        self.assertTrue(s.submit(req('base',customer='z',deadline=5))[0])
        s.tick()
        b.advance(2.1)
        s.tick()
        b.advance(3.45)
        s.tick()
        b.advance(4.8)
        s.tick()
        self.assertEqual(s.metrics['timing_bound_violations'],0)
        self.assertEqual(s.metrics['deadline_violations'],0)
        self.assertEqual(s.pending,0)

    def test_phase_accounting_snapshots_live_clock_once(self):
        class AdvancingClock:
            count=1
            capacity=32
            clock=0
            @property
            def now(self):
                self.clock+=.001
                return self.clock
        b=AdvancingClock()
        s=Scheduler(Config(replicas=1,max_batch_wait=30),b)
        s.submit(req('a'))
        s._account_phase_time()
        self.assertGreaterEqual(s.metrics['blocked_idle_seconds'],0)
        self.assertGreaterEqual(s.metrics['coalescing_idle_seconds'],0)
        self.assertAlmostEqual(s.metrics['queued_idle_seconds'],
            s.metrics['blocked_idle_seconds']+s.metrics['coalescing_idle_seconds'])


class ThreadedContract(unittest.TestCase):
    def test_completion_publication_and_drain_snapshot_are_atomic(self):
        class ImmediateEngine:
            def process_inputs(self,inputs):
                return [dict(id=q['id'],lora=None,output='done') for q in inputs]
            def load_lora(self,lora,path):
                return None
        class BarrierQueue:
            def __init__(self):
                self.inner=queue.SimpleQueue()
                self.entered=threading.Event()
                self.release=threading.Event()
            def put(self,completion):
                self.entered.set()
                if not self.release.wait(2):
                    raise AssertionError('completion publication barrier timed out')
                self.inner.put(completion)
            def get_nowait(self):
                return self.inner.get_nowait()
            def empty(self):
                return self.inner.empty()
        for kind in ('batch','load'):
            with self.subTest(kind=kind):
                b=ThreadedBackend(1,speed=1,engines=[ImmediateEngine()])
                barrier=BarrierQueue()
                b._done=barrier
                observer=None
                observed=[]
                entered=threading.Event()
                returned=threading.Event()
                try:
                    if kind=='batch':
                        b.start_batch(0,[{'id':'a'}],7)
                    else:
                        b.start_load(0,'a','/a',7)
                    self.assertTrue(barrier.entered.wait(1))
                    acquired=b._lock.acquire(blocking=False)
                    if acquired:
                        b._lock.release()
                    self.assertFalse(acquired,'publication must hold the state lock')
                    def observe():
                        entered.set()
                        observed.append(b.next_event())
                        returned.set()
                    observer=threading.Thread(target=observe)
                    observer.start()
                    self.assertTrue(entered.wait(1))
                    self.assertFalse(returned.wait(.02),
                                     'next_event must share the publication lock')
                finally:
                    barrier.release.set()
                    b.close()
                    if observer is not None:
                        observer.join(1)
                self.assertTrue(returned.is_set())
                self.assertIsNotNone(observed[0])
                self.assertEqual([c['token'] for c in b.poll()],[7])
                self.assertIsNone(b.next_event())

    def test_engine_wall_timestamps_are_anchored_to_monotonic_clock_drift(self):
        class Clocks:
            wall=100.0
            mono=1000.0
            def time(self):
                return self.wall
            def monotonic(self):
                return self.mono
        clock=Clocks()
        class Engine:
            events=[]
            def process_inputs(self,inputs):
                self.events.append(dict(ts=clock.wall,event='batch_start',size=1,lora=None))
                clock.wall+=31.00025
                clock.mono+=31
                self.events.extend([
                    dict(ts=clock.wall,event='request_done',id='a',lora=None),
                    dict(ts=clock.wall,event='batch_end',size=1,lora=None)])
                return [dict(id='a',lora=None,output='done')]
            def event_log(self):
                return self.events
        with patch('backends.time',clock):
            b=ThreadedBackend(1,speed=1,engines=[Engine()])
            b.start_batch(0,[{'id':'a'}],1)
            b.close()
            completion=b.poll()[0]
            event=b.event_log()[-1]
            self.assertLessEqual(event['ts'],completion['at'])
            self.assertAlmostEqual(event['ts'],31)
            self.assertAlmostEqual(event['raw_ts'],131.00025)

    def test_queued_completion_keeps_drain_wakeup_after_worker_finished(self):
        b=ThreadedBackend(1,speed=100)
        b.start_load(0,'a','/a',1)
        until=time.monotonic()+2
        while b._loading[0] and time.monotonic()<until:
            time.sleep(.001)
        self.assertIsNotNone(b.next_event())
        self.assertEqual(b.poll()[0]['token'],1)
        self.assertIsNone(b.next_event())
        b.close()

    def test_original_engine_backend_runs_homogeneous_work(self):
        b=ThreadedBackend(1,speed=100)
        s=Scheduler(Config(replicas=1,max_batch_wait=0,controller_margin=.5),b)
        s.submit(req('live',lora='live',at=b.now,deadline=b.now+20))
        until=time.monotonic()+2
        while s.pending and time.monotonic()<until:
            s.tick()
            time.sleep(.001)
        b.close()
        self.assertEqual(s.completed['live']['status'],'completed')
        self.assertEqual(b.engines[0].loaded_loras(),['live'])


if __name__=='__main__':
    unittest.main()
