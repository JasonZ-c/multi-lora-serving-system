"""Single-actor fair batching with conservative per-replica EDF commitments.

The immutable tickets are the admission ledger. Residency is an optimization:
waiting work always retains a reload allowance, including resident adapters.
Only actual foreground grants spend customer service credit. Admission and
runtime use the same nonpreemptible EDF fallback, including deadline-required idle
and loader blocking. All times are backend logical seconds.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from collections import defaultdict
import math

from engine import BATCH_SECONDS, COLD_LOAD_SECONDS, WARM_LOAD_SECONDS, MAX_BATCH_SIZE


@dataclass(frozen=True)
class Config:
    replicas: int = 4
    policy: str = 'fair'
    max_batch_wait: float = 30.0
    slo: float = 900.0
    starvation_threshold: float = 60.0
    tenant_ticket_cap: int = 32
    tenant_byte_cap: int = 32 * 1024 * 1024
    controller_margin: float = 0.0
    response_margin: float = 0.0
    demand_eviction: bool = True
    prefetch: bool = False
    replication: bool = False
    max_retries: int = 1
    weights: dict | None = None
    # When True, admission and dispatch protect already-accepted deadlines.
    # Replay sets this False so a fixed cohort still runs.
    admission_guard: bool = True
    record_events: bool = True
    prefetch_ttl: float = 30.0
    service_gap_guard: bool = True
    # ready: a customer competes only with a full or aged ticket.
    # pending: any legal small ticket makes that customer ready.
    eligibility: str = 'ready'

    def __post_init__(self):
        if not (0 <= self.max_batch_wait < self.slo and
                0 <= self.starvation_threshold < self.slo):
            raise ValueError('batch wait and customer gap trigger must each be in [0, SLO)')
        if self.replicas <= 0 or self.tenant_ticket_cap <= 0 or self.tenant_byte_cap <= 0:
            raise ValueError('replicas and tenant caps must be positive')
        if self.policy not in ('fifo', 'fair', 'locality'):
            raise ValueError('unknown scheduling policy')
        if self.eligibility not in ('ready', 'pending'):
            raise ValueError('eligibility must be ready or pending')
        if min(self.controller_margin, self.response_margin, self.max_retries) < 0:
            raise ValueError('margins and retries must be nonnegative')
        if self.weights and any(w <= 0 for w in self.weights.values()):
            raise ValueError('customer weights must be positive')


@dataclass(frozen=True)
class Request:
    id: str
    customer: str
    lora: str | None
    path: str | None
    prompt: str
    ingress: float
    deadline: float
    attempt: int = 1


@dataclass(frozen=True)
class Ticket:
    id: int
    customer: str
    lora: str | None
    path: str | None
    requests: tuple[Request, ...]
    replica: int
    # Upper bound charged on the replica's deadline ledger: 1.25s batch plus
    # cold (2s), warm (0.2s), or no load, plus the controller margin. Not the
    # measured runtime. Pair scoring subtracts it so the ticket is not counted twice.
    reserved_work: float
    deadline: float
    first_ingress: float
    attempt: int = 1
    retries: int = 0


@dataclass
class Customer:
    virtual_service: float = 0.0
    backlog_since: float | None = None
    last_grant: float | None = None
    # Accepted requests not yet published, including the one currently running.
    outstanding: int = 0
    # Tickets still in a replica queue, before a foreground grant. The ticket cap counts these.
    waiting_tickets: int = 0
    # Prompt bytes of outstanding requests. The byte cap counts these.
    queued_bytes: int = 0
    # Accepted requests whose ticket has not been granted. Zero clears the service-gap clock.
    unstarted_requests: int = 0


@dataclass
class CacheEntry:
    last_use: float


@dataclass
class Replica:
    id: int
    waiting: dict = field(default_factory=dict)
    reserved_total: float = 0.0
    foreground: int | None = None
    # 'load' or 'compute' while the foreground ticket is in progress; None when idle.
    phase: str | None = None
    # Fence for the engine call in progress. A completion with a different token is stale.
    token: object = None
    # Logical time the engine gave for the end of the current compute batch.
    foreground_end: float = 0.0
    foreground_bound: float = 0.0
    grant_at: float = 0.0
    compute_at: float | None = None
    fair_reserved: float = 0.0
    loading: dict | None = None
    resident: dict = field(default_factory=dict)
    warm: set = field(default_factory=set)
    # adapter -> logical time until a prefetch pin blocks ordinary eviction.
    pins: dict = field(default_factory=dict)
    # The waiting queue's last deadline check stays valid until this time.
    # -inf means the next join must run the check again.
    certified_until: float = -math.inf
    timing_unhealthy: bool = False


class Scheduler:
    def __init__(self, config, backend):
        if backend.count != config.replicas:
            raise ValueError('backend replica count differs from configuration')
        self.config = config
        self.backend = backend
        self.replicas = [Replica(i) for i in range(config.replicas)]
        self.tickets = {}
        self.customers = {}
        self.completed = {}
        self.events = []
        self.metrics = defaultdict(int)
        for name in ('accepted','rejected','batches','loads','evictions','cache_hits',
                     'prefetches','replications','edf_overrides','deadline_violations',
                     'failed','stale_completions','retries','max_service_gap',
                     'timing_bound_violations','idle_seconds','queued_idle_seconds',
                     'coalescing_idle_seconds','blocked_idle_seconds'):
            self.metrics[name] = 0
        self._request_ticket = {}
        self._tails = {}
        self._fifo_tail = None
        self._ticket_seq = 0
        self._token_seq = 0
        self.pending = 0
        self._accounted_at = self.now

    @property
    def now(self):
        return self.backend.now

    def _event(self, event, **fields):
        if self.config.record_events:
            self.events.append(dict(ts=self.now, event=event, **fields))

    def _token(self):
        self._token_seq += 1
        return self._token_seq

    def _weight(self, customer):
        return (self.config.weights or {}).get(customer, 1.0)

    def _reserve(self, lora, replica):
        """Seconds charged for one ticket: batch + cold/warm load + controller margin."""
        load = 0.0 if lora is None else (WARM_LOAD_SECONDS if lora in replica.warm
                                         else COLD_LOAD_SECONDS)
        return BATCH_SECONDS + load + self.config.controller_margin

    def _foreground_remaining(self, r):
        if r.foreground is None:
            return 0.0
        # One controller allowance covers the entire committed sequence. A
        # delayed load callback must not mint another allowance for compute.
        return max(0.0, r.foreground_bound - self.now)

    def _timing_healthy(self, r):
        """True while every active load or compute is still inside its bound."""
        healthy = not (
            (r.foreground is not None and self.now > r.foreground_bound+1e-7) or
            (r.loading is not None and
             self.now > r.loading['end']+self.config.controller_margin+1e-7))
        if not healthy and not r.timing_unhealthy:
            self.metrics['timing_bound_violations'] += 1
            self._event('timing_bound_exceeded',replica=r.id,phase=r.phase,
                        foreground_bound=r.foreground_bound,
                        loader_bound=r.loading['end']+self.config.controller_margin
                        if r.loading else None)
        r.timing_unhealthy = not healthy
        return healthy

    def _loader_remaining(self, r):
        return max(0.0, r.loading['end'] + self.config.controller_margin - self.now) if r.loading else 0.0

    def _initial(self, r, loader_end=None):
        """Earliest logical time new waiting work on this replica can start."""
        remaining = max(self._foreground_remaining(r), self._loader_remaining(r))
        if loader_end is not None:
            remaining = max(remaining, loader_end + self.config.controller_margin - self.now)
        return self.now + remaining

    def _feasible(self, r, candidate=None, remove=None, loader_end=None,
                  certify=False):
        """True if deadline order still finishes every accepted ticket on time.

        False is a sufficient-check failure, not proof that no schedule exists.
        """
        if not self.config.admission_guard:
            return True
        if not self._timing_healthy(r):
            return False
        if r.foreground is not None:
            fg = self.tickets[r.foreground]
            if self.now + self._foreground_remaining(r) + self.config.response_margin > fg.deadline + 1e-9:
                return False
        waiting = [t for t in r.waiting.values()
                   if t.id != remove and (candidate is None or t.id != candidate.id)]
        if candidate is not None:
            waiting.append(candidate)
        t_end = self._initial(r, loader_end)
        initial = t_end
        slack = math.inf
        for ticket in sorted(waiting, key=lambda t: (t.deadline, t.id)):
            t_end += ticket.reserved_work
            if t_end + self.config.response_margin > ticket.deadline + 1e-9:
                return False
            slack = min(slack,ticket.deadline-self.config.response_margin-t_end)
        if certify:
            r.certified_until = initial+slack
            if r.foreground is not None:
                r.certified_until = min(r.certified_until,
                    self.tickets[r.foreground].deadline-self.config.response_margin,
                    r.foreground_bound)
            if r.loading is not None:
                r.certified_until = min(r.certified_until,
                    r.loading['end']+self.config.controller_margin)
        return True

    def _replace_waiting(self, old, new):
        r = self.replicas[old.replica]
        if old.reserved_work != new.reserved_work or old.deadline != new.deadline:
            r.certified_until = -math.inf
        r.reserved_total += new.reserved_work - old.reserved_work
        r.waiting[old.id] = new
        self.tickets[old.id] = new

    def _add_waiting(self, ticket):
        r = self.replicas[ticket.replica]
        r.certified_until = -math.inf
        r.waiting[ticket.id] = ticket
        r.reserved_total += ticket.reserved_work
        self.tickets[ticket.id] = ticket

    def _remove_waiting(self, ticket):
        r = self.replicas[ticket.replica]
        r.certified_until = -math.inf
        r.waiting.pop(ticket.id)
        r.reserved_total = max(0.0, r.reserved_total - ticket.reserved_work)

    def _reject(self, reason):
        self.metrics['rejected'] += 1
        self.metrics['rejected_' + reason] += 1
        return False, reason

    def submit(self, request, force=False):
        self._account_phase_time()
        if request.id in self._request_ticket or request.id in self.completed:
            return True, 'duplicate'
        if request.deadline < request.ingress or not math.isfinite(request.deadline):
            return self._reject('invalid_request')
        if request.lora is not None and not request.path:
            return self._reject('invalid_request')
        account = self.customers.setdefault(request.customer, Customer())
        size = len(request.prompt.encode('utf-8'))
        if not force and account.queued_bytes + size > self.config.tenant_byte_cap:
            return self._reject('tenant_limit')
        key = (request.customer, request.lora, request.path)
        tail_id = self._fifo_tail if self.config.policy == 'fifo' else self._tails.get(key)
        tail = self.tickets.get(tail_id)
        if tail and (tail.customer,tail.lora,tail.path) == key and len(tail.requests) < MAX_BATCH_SIZE:
            r = self.replicas[tail.replica]
            if tail.id in r.waiting:
                joined = replace(tail, requests=tail.requests + (request,),
                                 deadline=min(tail.deadline,request.deadline),
                                 first_ingress=min(tail.first_ingress,request.ingress))
                # An unchanged EDF ledger has a cached latest safe idle time.
                # Recheck once that certificate expires, including controller lag.
                if force or not self.config.admission_guard:
                    safe = True
                elif joined.deadline == tail.deadline:
                    safe = self._timing_healthy(r) and (self.now <= r.certified_until
                                                       or self._feasible(r,certify=True))
                else:
                    safe = self._feasible(r,joined)
                if force or safe:
                    self._replace_waiting(tail, joined)
                    self._accept_request(request, account, size, joined.id)
                    return True, 'accepted'
        if not force and account.waiting_tickets >= self.config.tenant_ticket_cap:
            return self._reject('tenant_limit')
        ticket_id = self._ticket_seq + 1
        choices = []
        for r in self.replicas:
            ticket = Ticket(ticket_id,request.customer,request.lora,request.path,
                            (request,),r.id,self._reserve(request.lora,r),
                            request.deadline,request.ingress,request.attempt)
            if force or self._feasible(r,ticket):
                if self.config.policy == 'locality':
                    score = self._pair_cost(ticket,r)
                else:
                    score = self._foreground_remaining(r) + r.reserved_total + ticket.reserved_work
                choices.append((score,r.id,ticket))
        if not choices:
            lower = BATCH_SECONDS + (0 if request.lora is None else WARM_LOAD_SECONDS)
            reason = ('no_healthy_replica' if not any(self._timing_healthy(r) for r in self.replicas)
                      else 'deadline_infeasible' if self.now+lower+self.config.response_margin > request.deadline
                      else 'conservative_capacity')
            return self._reject(reason)
        ticket = min(choices,key=lambda c:(c[0],c[1]))[2]
        self._ticket_seq = ticket_id
        self._add_waiting(ticket)
        self._tails[key] = ticket.id
        self._fifo_tail = ticket.id
        account.waiting_tickets += 1
        self._accept_request(request,account,size,ticket.id)
        return True,'accepted'

    def _accept_request(self, request, account, size, ticket_id):
        if not account.outstanding:
            active = [a.virtual_service for a in self.customers.values() if a.outstanding]
            account.virtual_service = min(active) if active else account.virtual_service
            account.last_grant = None
        if not account.unstarted_requests:
            account.backlog_since = self.now
        account.unstarted_requests += 1
        account.outstanding += 1
        account.queued_bytes += size
        self._request_ticket[request.id] = ticket_id
        self.pending += 1
        self.metrics['accepted'] += 1

    def cancel(self, request_id):
        self._account_phase_time()
        tid = self._request_ticket.get(request_id)
        ticket = self.tickets.get(tid)
        if ticket is None or tid not in self.replicas[ticket.replica].waiting:
            return False
        request = next(q for q in ticket.requests if q.id == request_id)
        remaining = tuple(q for q in ticket.requests if q.id != request_id)
        if remaining:
            new = replace(ticket,requests=remaining,
                          deadline=min(q.deadline for q in remaining),
                          first_ingress=min(q.ingress for q in remaining))
            self._replace_waiting(ticket,new)
        else:
            self._remove_waiting(ticket)
            del self.tickets[tid]
            self.customers[ticket.customer].waiting_tickets -= 1
        account = self.customers[request.customer]
        account.unstarted_requests -= 1
        if not account.unstarted_requests:
            account.backlog_since = None
        self._release_request(request)
        return True

    def _release_request(self, request):
        self._request_ticket.pop(request.id,None)
        a = self.customers[request.customer]
        a.outstanding -= 1
        a.queued_bytes -= len(request.prompt.encode('utf-8'))
        if not a.outstanding:
            a.backlog_since = None
        self.pending -= 1

    def _exposed_load(self, ticket, r):
        """Seconds this grant would block before compute. Zero when the adapter is resident."""
        if ticket.lora is None or ticket.lora in r.resident:
            return 0.0
        if r.loading and r.loading['lora'] == ticket.lora:
            return self._loader_remaining(r)
        return WARM_LOAD_SECONDS if ticket.lora in r.warm else COLD_LOAD_SECONDS

    def _victim(self, r, demand=False):
        """Resident adapter to evict so a new one can load, or None.

        This is the on-demand LRU choice. demand=True is a foreground load and
        ignores prefetch pins. demand=False is prefetch and keeps an unexpired
        pin. Loading and in-use adapters are never candidates. With
        demand_eviction, an adapter a waiting ticket still needs is kept until
        the others are gone; the rest are least-recently-used.
        """
        excluded = set()
        if r.loading:
            excluded.add(r.loading['lora'])
        if r.foreground is not None:
            excluded.add(self.tickets[r.foreground].lora)
        candidates = [lora for lora in r.resident if lora not in excluded
                      and (demand or r.pins.get(lora,0) <= self.now)]
        if not candidates:
            return None
        queued = {t.lora for t in r.waiting.values()} if self.config.demand_eviction else set()
        # queued==False sorts first, then oldest last_use.
        return min(candidates,key=lambda l:(l in queued,
                   r.resident[l].last_use,l))

    def _pair_cost(self, ticket, r):
        """Lower is better: replica backlog plus this ticket's load, per request."""
        other = r.reserved_total
        if ticket.id in r.waiting:
            other -= ticket.reserved_work
        # Scoring is a backlog proxy, distinct from admission's remaining
        # sequence. A granted loading ticket remains compute-unstarted work.
        running = 0.0
        if r.foreground is not None:
            if r.phase == 'compute':
                running = max(0.0,r.foreground_end-self.now)
            else:
                other += self.tickets[r.foreground].reserved_work
        wait = running + max(0.0,other)
        load = self._exposed_load(ticket,r)
        return (wait + load + BATCH_SECONDS) / len(ticket.requests)

    def _legal(self, ticket, r):
        """True when this idle replica can start the ticket now, including a needed load."""
        if r.foreground is not None:
            return False
        if ticket.lora is None or ticket.lora in r.resident:
            return True
        if r.loading:
            return r.loading['lora'] == ticket.lora
        if len(r.resident) < self.backend.capacity:
            return True
        return self._victim(r,demand=True) is not None

    def _batch_ready(self, ticket):
        """Full (128) or aged to MAX_BATCH_WAIT.

        Under eligibility='ready' this is the customer gate: a small young
        ticket does not make its customer ready. Under eligibility='pending'
        the same predicate is only a preference after the customer is chosen.
        """
        return len(ticket.requests) == MAX_BATCH_SIZE or self.now >= ticket.first_ingress+self.config.max_batch_wait-1e-9

    def _gap(self, account):
        """Seconds this customer has had backlog since its last foreground grant."""
        if account.backlog_since is None:
            return 0.0
        return self.now - max(account.backlog_since,
                              account.last_grant if account.last_grant is not None else account.backlog_since)

    def _safe_first(self, r, first):
        """True if running `first` now still meets every accepted deadline on this replica.

        False means the fair choice would miss a deadline, so the caller grants
        the earliest-deadline ticket instead.
        """
        if not self.config.admission_guard:
            return True
        t_end = self.now + BATCH_SECONDS + self._exposed_load(first,r) + self.config.controller_margin
        if t_end + self.config.response_margin > first.deadline + 1e-9:
            return False
        # A prefetch can overlap this compute, but may block the next load.
        t_end = max(t_end,self.now+self._loader_remaining(r))
        for ticket in sorted(r.waiting.values(),key=lambda t:(t.deadline,t.id)):
            if ticket.id == first.id:
                continue
            t_end += ticket.reserved_work
            if t_end + self.config.response_margin > ticket.deadline+1e-9:
                return False
        return True

    def _idle_slack(self, r):
        """Seconds this replica can stay idle before an accepted deadline is at risk.

        Zero means waiting any longer would miss, so tick grants the earliest deadline.
        """
        if not self.config.admission_guard or not r.waiting:
            return math.inf
        t_end = self._initial(r)
        slack = math.inf
        for ticket in sorted(r.waiting.values(),key=lambda t:(t.deadline,t.id)):
            t_end += ticket.reserved_work
            slack = min(slack,ticket.deadline-self.config.response_margin-t_end)
        return max(0.0,slack)

    def _choose(self, pairs):
        """Pick one legal (ticket, replica). Returns that pair; it does not grant it."""
        if self.config.policy == 'fifo':
            return min(pairs,key=lambda p:(p[0].id,p[1].id))
        customers = {t.customer for t,r in pairs}
        overdue = [c for c in customers if self.config.service_gap_guard and
                   self._gap(self.customers[c]) >= self.config.starvation_threshold]
        if overdue:
            customer = min(overdue,key=lambda c:(-self._gap(self.customers[c]),c))
            self.metrics['service_gap_overrides'] += 1
        else:
            customer = min(customers,key=lambda c:(self.customers[c].virtual_service,c))
        eligible = [(t,r) for t,r in pairs if t.customer == customer]
        if self.config.eligibility == 'pending':
            preferred = [(t,r) for t,r in eligible if self._batch_ready(t)]
            eligible = preferred or eligible
        if self.config.policy == 'locality':
            return min(eligible,key=lambda p:(self._pair_cost(*p),p[0].first_ingress,
                       self._exposed_load(*p),p[0].id,p[1].id))
        return min(eligible,key=lambda p:(p[0].first_ingress,p[0].id,p[1].id))

    def _evict_for(self, r, demand):
        if len(r.resident) < self.backend.capacity:
            return True
        victim = self._victim(r,demand=demand)
        if victim is None:
            return False
        self.backend.evict(r.id,victim)
        r.resident.pop(victim)
        r.pins.pop(victim,None)
        self.metrics['evictions'] += 1
        self._event('evict',replica=r.id,lora=victim)
        return True

    def _start_load(self, r, ticket, prefetch=False):
        if not self._evict_for(r, demand=not prefetch):
            return False
        token = self._token()
        end = self.backend.start_load(r.id,ticket.lora,ticket.path,token)
        r.loading = dict(lora=ticket.lora,path=ticket.path,end=end,token=token,prefetch=prefetch)
        r.certified_until = -math.inf
        if prefetch:
            r.pins[ticket.lora] = self.now+self.config.prefetch_ttl
            self.metrics['prefetches'] += 1
        return True

    def _compute(self, r):
        ticket = self.tickets[r.foreground]
        r.phase = 'compute'
        r.token = self._token()
        r.compute_at = self.now
        r.certified_until = -math.inf
        self.metrics['exposed_load_seconds'] += self.now-r.grant_at
        inputs = [dict(id=q.id,customer=q.customer,lora=q.lora,prompt=q.prompt)
                  for q in ticket.requests]
        try:
            r.foreground_end = self.backend.start_batch(r.id,inputs,r.token)
        except Exception as exc:
            self._retry(r,f'{type(exc).__name__}: {exc}',self.now)
            return
        if ticket.lora in r.resident:
            entry = r.resident[ticket.lora]
            entry.last_use = self.now
            r.pins.pop(ticket.lora,None)
        self.metrics['batches'] += 1
        self._event('batch_start',replica=r.id,ticket=ticket.id,customer=ticket.customer,
                    lora=ticket.lora,size=len(ticket.requests),attempt=ticket.attempt)

    def _grant(self, ticket, r):
        self._remove_waiting(ticket)
        account = self.customers[ticket.customer]
        account.waiting_tickets -= 1
        gap = self._gap(account)
        self.metrics['max_service_gap'] = max(self.metrics['max_service_gap'],gap)
        account.last_grant = self.now
        account.unstarted_requests -= len(ticket.requests)
        if not account.unstarted_requests:
            account.backlog_since = None
        r.foreground = ticket.id
        r.grant_at = self.now
        if r.loading and r.loading['lora'] == ticket.lora:
            # The loader's allowance is already in _exposed_load's blocking
            # bound. Keep the original remaining sequence rather than adding
            # that allowance a second time or restarting its clock.
            r.foreground_bound = r.loading['end']+BATCH_SECONDS+self.config.controller_margin
        else:
            r.foreground_bound = (r.grant_at+BATCH_SECONDS+self._exposed_load(ticket,r)
                                  +self.config.controller_margin)
        r.compute_at = None
        r.fair_reserved = (BATCH_SECONDS+self._exposed_load(ticket,r))/self._weight(ticket.customer)
        account.virtual_service += r.fair_reserved
        self._event('grant',replica=r.id,ticket=ticket.id,customer=ticket.customer,
                    lora=ticket.lora,size=len(ticket.requests),gap=gap,
                    reservation=r.fair_reserved,attempt=ticket.attempt)
        if ticket.lora is None or ticket.lora in r.resident:
            self.metrics['cache_hits'] += int(ticket.lora is not None)
            self._compute(r)
        else:
            r.phase = 'load'
            if not r.loading:
                try:
                    self._start_load(r,ticket)
                except Exception as exc:
                    self._retry(r,f'{type(exc).__name__}: {exc}',self.now)
                    return
            r.foreground_end = r.loading['end'] + BATCH_SECONDS

    def _reconcile(self, r, end):
        ticket = self.tickets[r.foreground]
        actual = max(0.0,end-r.grant_at)/self._weight(ticket.customer)
        self.customers[ticket.customer].virtual_service += actual-r.fair_reserved

    def _finish(self, r, completion):
        self._timing_healthy(r)
        ticket = self.tickets[r.foreground]
        engine_end = completion.get('at',self.now)
        self._reconcile(r,engine_end)
        outputs = {str(o['id']):o for o in completion.get('result',[]) or []}
        for request in ticket.requests:
            output = outputs.get(str(request.id))
            error = None if output is not None else 'engine omitted accepted result'
            self._publish(request,ticket,r.id,r.compute_at,self.now,output,error,
                          engine_end=engine_end)
        del self.tickets[ticket.id]
        r.foreground = None
        r.certified_until = -math.inf
        r.phase = None
        r.token = None
        r.compute_at = None

    def _publish(self, request, ticket, replica, start, end, output, error,
                 engine_end=None):
        if request.id in self.completed:
            return
        status = 'failed' if error else 'completed'
        outcome = dict(id=request.id,customer=request.customer,lora=request.lora,
                       ingress=request.ingress,deadline=request.deadline,status=status,
                       output=output,result=output,error=error,start=start,end=end,
                       completed_at=end,engine_end=end if engine_end is None else engine_end,
                       replica=replica,attempt=ticket.attempt)
        self.completed[request.id] = outcome
        self.metrics['failed'] += int(bool(error))
        self.metrics['deadline_violations'] += int(bool(error) or end > request.deadline+1e-9)
        self._release_request(request)

    def _retry(self, r, error, end):
        ticket = self.tickets[r.foreground]
        self._reconcile(r,end)
        start = r.compute_at
        r.foreground = None
        r.certified_until = -math.inf
        r.phase = None
        r.token = None
        if ticket.retries < self.config.max_retries:
            choices = []
            for target in self.replicas:
                candidate = replace(ticket,replica=target.id,
                                    reserved_work=self._reserve(ticket.lora,target),
                                    attempt=ticket.attempt+1,retries=ticket.retries+1)
                if self._feasible(target,candidate):
                    choices.append((self._initial(target)+target.reserved_total+candidate.reserved_work,
                                    target.id,candidate))
            if choices:
                candidate = min(choices,key=lambda x:(x[0],x[1]))[2]
                self._add_waiting(candidate)
                self.customers[ticket.customer].waiting_tickets += 1
                account = self.customers[ticket.customer]
                if not account.unstarted_requests:
                    account.backlog_since = self.now
                account.unstarted_requests += len(ticket.requests)
                self.metrics['retries'] += 1
                self._event('retry',ticket=ticket.id,attempt=candidate.attempt,error=error)
                return
        for request in ticket.requests:
            self._publish(request,ticket,r.id,start,self.now,None,error,engine_end=end)
        del self.tickets[ticket.id]

    def _completion(self, completion):
        r = self.replicas[completion['replica']]
        if completion['kind'] == 'load':
            if not r.loading or completion['token'] != r.loading['token']:
                self.metrics['stale_completions'] += 1
                return
            load = r.loading
            r.loading = None
            r.certified_until = -math.inf
            if completion['ok']:
                lora = load['lora']
                r.resident[lora] = CacheEntry(self.now)
                r.warm.add(lora)
                self.metrics['loads'] += 1
                # A completed cold load safely changes future reload bounds to warm.
                for old in list(r.waiting.values()):
                    if old.lora == lora:
                        self._replace_waiting(old,replace(old,reserved_work=self._reserve(lora,r)))
                if r.foreground is not None and self.tickets[r.foreground].lora == lora:
                    self._compute(r)
            else:
                r.pins.pop(load['lora'],None)
                if r.foreground is not None and self.tickets[r.foreground].lora == load['lora']:
                    self._retry(r,completion.get('error','load failed'),completion.get('at',self.now))
        else:
            if r.foreground is None or r.phase != 'compute' or completion['token'] != r.token:
                self.metrics['stale_completions'] += 1
                return
            if completion['ok']:
                self._finish(r,completion)
            else:
                self._retry(r,completion.get('error','batch failed'),completion.get('at',self.now))

    def migrate(self, ticket_id, replica):
        ticket = self.tickets.get(ticket_id)
        if ticket is None or ticket_id not in self.replicas[ticket.replica].waiting:
            return False
        if ticket.replica == replica:
            return True
        source = self.replicas[ticket.replica]
        target = self.replicas[replica]
        candidate = replace(ticket,replica=replica,reserved_work=self._reserve(ticket.lora,target))
        if not self._feasible(source,remove=ticket.id) or not self._feasible(target,candidate):
            return False
        self._remove_waiting(ticket)
        self._add_waiting(candidate)
        self._event('migrate',ticket=ticket_id,source=source.id,replica=replica)
        return True

    def _replicate(self):
        if not self.config.replication or self.config.policy != 'locality':
            return
        for target in self.replicas:
            if target.foreground is not None:
                continue
            candidates = []
            for ticket in self.tickets.values():
                source = self.replicas[ticket.replica]
                if source is target or ticket.id not in source.waiting:
                    continue
                # Copy only filled batches, and only when genuine source waiting
                # exceeds loading on the destination; no movement based on history.
                if len(ticket.requests) < MAX_BATCH_SIZE:
                    continue
                saved = self._foreground_remaining(source)+source.reserved_total-ticket.reserved_work
                added = self._exposed_load(ticket,target)
                if saved > added+WARM_LOAD_SECONDS and self._legal(ticket,target):
                    candidates.append((-saved+added,ticket.id))
            for _,tid in sorted(candidates):
                if self.migrate(tid,target.id):
                    self.metrics['replications'] += 1
                    break

    def _prefetch(self):
        if not self.config.prefetch or self.config.policy != 'locality':
            return
        for r in self.replicas:
            r.pins = {l:t for l,t in r.pins.items() if t > self.now}
            if r.loading or r.phase != 'compute' or len(r.pins) >= 2:
                continue
            for ticket in sorted(r.waiting.values(),key=lambda t:t.id)[:2]:
                if ticket.lora is None or ticket.lora in r.resident:
                    continue
                end = self.now+self._exposed_load(ticket,r)
                if not self._feasible(r,loader_end=end):
                    continue
                try:
                    if self._start_load(r,ticket,prefetch=True):
                        break
                except Exception as exc:
                    self._event('prefetch_error',replica=r.id,error=str(exc))

    def tick(self, now=None):
        """Grant every idle replica that has legal work. Nothing is returned.

        A fair choice that fails _safe_first is replaced by that replica's
        earliest-deadline ticket. With no eligible ticket, the replica stays
        idle until _idle_slack hits zero.
        """
        if now is not None and abs(now-self.now) > 1e-6:
            raise ValueError('advance the backend clock before tick(now)')
        self._account_phase_time()
        for r in self.replicas:
            self._timing_healthy(r)
        for completion in self.backend.poll():
            self._completion(completion)
        self._replicate()
        # Each grant reserves service before the next worker chooses a customer.
        # Replicas skipped for the rest of this tick. A replica lands here when
        # its earliest-deadline ticket cannot start because a different adapter
        # is already loading. Other replicas can still be granted.
        loader_blocked = set()
        while True:
            pairs = [(ticket,r) for r in self.replicas if r.foreground is None and r.id not in loader_blocked
                     for ticket in r.waiting.values()
                     if self._legal(ticket,r) and (self.config.eligibility == 'pending' or self._batch_ready(ticket))]
            forced = False
            if pairs:
                ticket,r = self._choose(pairs)
                if not self._safe_first(r,ticket):
                    ticket = min(r.waiting.values(),key=lambda t:(t.deadline,t.id))
                    forced = True
                    self.metrics['edf_overrides'] += 1
            else:
                urgent = [(min(r.waiting.values(),key=lambda t:(t.deadline,t.id)),r)
                          for r in self.replicas if r.foreground is None and r.waiting and r.id not in loader_blocked
                          and self._idle_slack(r) <= 1e-8]
                if not urgent:
                    break
                ticket,r = min(urgent,key=lambda p:(p[0].deadline,p[0].id))
                forced = True
                self.metrics['edf_overrides'] += 1
            if not self._legal(ticket,r):
                # An unrelated loader temporarily blocks this EDF head. Its end
                # is a wakeup and its blocking was checked before it started.
                loader_blocked.add(r.id)
                continue
            # forced means we already fell back to the earliest deadline, and
            # that ticket still misses. admission_guard only records the broken
            # plan; the grant below still happens. Replay leaves the guard off.
            if forced and not self._safe_first(r,ticket) and self.config.admission_guard:
                self._event('feasibility_lost',replica=r.id,ticket=ticket.id)
            self._grant(ticket,r)
        self._prefetch()

    def _account_phase_time(self):
        """Accrue replica-seconds before a new arrival or controller mutation."""
        previous = self._accounted_at
        now = self.now
        delta = max(0.0,now-previous)
        if not delta:
            return
        for r in self.replicas:
            if r.foreground is not None:
                continue
            self.metrics['idle_seconds'] += delta
            if not r.waiting:
                continue
            self.metrics['queued_idle_seconds'] += delta
            if self.config.eligibility == 'pending':
                # A small ticket is already eligible, so this idle is not coalescing.
                self.metrics['blocked_idle_seconds'] += delta
            else:
                ready_at = min(-math.inf if self._batch_ready(t) else
                               t.first_ingress+self.config.max_batch_wait
                               for t in r.waiting.values())
                coalescing = max(0.0,min(now,ready_at)-previous)
                self.metrics['coalescing_idle_seconds'] += coalescing
                self.metrics['blocked_idle_seconds'] += delta-coalescing
        self._accounted_at = now

    def next_wakeup(self):
        """Next logical time to tick, or None when nothing accepted is still in flight."""
        wakes = []
        event = self.backend.next_event()
        if event is not None:
            wakes.append(event)
        for r in self.replicas:
            if r.foreground is not None:
                continue
            if not r.waiting:
                continue
            future = [t.first_ingress+self.config.max_batch_wait for t in r.waiting.values()
                      if t.first_ingress+self.config.max_batch_wait > self.now+1e-9]
            if future:
                wakes.append(min(future))
            slack = self._idle_slack(r)
            if math.isfinite(slack) and slack > 1e-8:
                wakes.append(self.now+slack)
        # Service gaps are checked at every legal opportunity. A busy fleet
        # cannot grant when a gap timer expires; its completion already wakes us.
        return min(wakes) if wakes else None

    def request_status(self, request_id):
        if request_id in self.completed:
            return self.completed[request_id]
        tid = self._request_ticket.get(request_id)
        if tid is None:
            return None
        ticket = self.tickets[tid]
        r = self.replicas[ticket.replica]
        return dict(id=request_id,status='running' if r.foreground == tid else 'queued',
                    replica=ticket.replica,attempt=ticket.attempt,start=r.compute_at
                    if r.foreground == tid else None)
