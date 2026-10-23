# System Walkthrough Illustrations

## 1. Request Ingress & Immutable Version Pinning

### End-to-End Ingress Flow

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant H as HTTP Handler (http_api.py)
    participant S as ProductService (service.py)
    participant DB as SQLite Store (store.py)
    participant Sch as Scheduler (scheduler.py)

    C->>H: POST /v1/inferences (Bearer KEY, model="endpoint:support")
    Note over H: 1. Capture ingress timestamp<br/>2. Authenticate API key
    H->>S: service.infer(key, body, idempotency_key, ingress)
    Note over S: Acquire serialized actor lock
    S->>DB: Check Idempotency-Key (customer-scoped)
    S->>DB: store.resolve(customer, "endpoint:support")
    DB-->>S: Resolved & Pinned: {model: "lora:support@v1", lora: '["acme","support","v1"]', path: "s3://.../v1"}
    S->>Sch: scheduler.submit(Request(lora='["acme","support","v1"]', path="s3://.../v1", deadline=ingress+900))
    Sch-->>S: (True, "accepted") — Ticket reserved in r.waiting
    S->>DB: store.insert_request(record) [synchronous=FULL]
    Note over S,Sch: If DB commit fails -> scheduler.cancel(id)
    S-->>H: 202 Accepted + pinned record
    H-->>C: 202 Accepted (Location: /v1/requests/{id})
```

### How Version Pinning Survives a Live Upgrade (`v1` → `v2`)

```mermaid
flowchart TB
    subgraph Registry["SQLite Mutable Route vs. Immutable Versions"]
        V1["versions table:\n(acme, support, v1) -> s3://.../v1"]
        V2["versions table:\n(acme, support, v2) -> s3://.../v2"]
        EP["endpoints table:\nendpoint:support\n(pointer moves v1 -> v2)"]
    end

    subgraph Flight["In-Flight Tickets & Requests (Immutable)"]
        R1["Request 1 (arrived at t=10s)\nPinned at admission:\nmodel = lora:support@v1\nlora = '[\"acme\",\"support\",\"v1\"]'\npath = s3://.../v1"]
        R2["Request 2 (arrived at t=12s, after release)\nPinned at admission:\nmodel = lora:support@v2\nlora = '[\"acme\",\"support\",\"v2\"]'\npath = s3://.../v2"]
    end

    V1 -. "1. Resolved at t=10s" .-> R1
    EP == "At t=11s: release(v2, expected_revision=1)" ==> V2
    V2 -. "2. Resolved at t=12s" .-> R2
```

---

## 2. Handling the Whale Customer (`c00`, 40% Traffic) vs. Small Customer (`c49`, 0.02% Traffic)

### Three-Line Defense Against Whale Monopoly (Admission + Dispatch)

```mermaid
flowchart LR
    subgraph Arrivals["Incoming Traffic (1,000 req/s Burst)"]
        W["Whale c00\n(400 req/s, 10 LoRAs)"]
        S["Small c49\n(~0.05 req/s, 2 LoRAs)"]
    end

    subgraph Defense1["Defense 1: Admission Isolation (submit)"]
        Cap["Per-Customer Ticket Cap\n(waiting_tickets <= 32)\nMax 32 * 128 = 4,096 queued reqs\n(~46s max EDF backlog per customer)"]
        Rej["429 tenant_limit\n(Whale excess shed;\nEDF headroom preserved for c49)"]
    end

    subgraph Defense2["Defense 2 & 3: Customer-First Dispatch (_choose & _grant)"]
        Gate["Readiness Gate\n(Full 128 OR age >= 30s)"]
        Gap{"Any ready customer\nservice_gap >= 60s?"}
        VS["Pick min(virtual_service)\n+ Immediate Reservation on _grant\n+ Rejoin at min(active)"]
        Override["Pick max(service_gap)\n(Starvation Guard Override)"]
    end

    W --> Cap
    Cap -- "waiting_tickets == 32" --> Rej
    Cap -- "waiting_tickets < 32" --> Gate
    S -- "waiting_tickets == 0 (100% admitted!)" --> Gate
    Gate --> Gap
    Gap -- "Yes (e.g., c49 waited 60s)" --> Override
    Gap -- "No" --> VS
```

### Idle Fleet vs. Overloaded Fleet Behavior

```mermaid
flowchart TB
    subgraph Idle["Case A: Fleet is Idle (Only Whale c00 is Active)"]
        I1["c00 is the ONLY eligible customer"] --> I2["c00 receives grants on ALL 4 Replicas\n(100% fleet utilization, 409.6 req/s)"]
    end

    subgraph Overload["Case B: 1,000 req/s Burst (All 50 Customers Active)"]
        O1["c00 rapidly accumulates Virtual Service\n& hits 32 waiting_tickets cap"] --> O2["c49 arrives: joins at min(active virtual_service)\n& passes EDF easily"]
        O2 --> O3["Once c49's ticket ages to 30s (ready),\nits low virtual_service or 60s gap guard\nwins the very next replica grant!"]
    end
```

---

## 3. Ticket Lifecycle & State Accounting (`r.waiting` vs. `r.foreground`)

**Key Insight:** There is **NO separate customer queue**. Your mental steps "1. parking in customer backlog" and "2. enqueued in `r.waiting`" happen **in the exact same place (`r.waiting`)**!

```mermaid
stateDiagram-v2
    direction LR
    [*] --> InReplicaWaiting : submit() -> _add_waiting(ticket)

    state "Stage 1: In r.waiting[ticket.id] (Unstarted)" as InReplicaWaiting {
        Coalescing : 1A. Coalescing (_batch_ready == False)\nsize < 128 AND age < 30s\nAbsorbs new requests in-place
        Ready : 1B. Ready (_batch_ready == True)\nsize == 128 OR age >= 30s\nCompetes in _choose() fairness
        Coalescing --> Ready : reaches 128 reqs OR 30s timer
    }

    state "Stage 2: In r.foreground == ticket.id (Executing)" as InForeground {
        Loading : 2A. r.phase == 'load'\nCold (2.0s) or Warm (0.2s) load
        Computing : 2B. r.phase == 'compute'\nGPU batch execution (1.25s)
        Loading --> Computing : load_end callback
    }

    InReplicaWaiting --> InForeground : tick() -> _grant(ticket, r)\n[_remove_waiting(ticket)\naccount.waiting_tickets -= 1\nr.reserved_total -= ticket.reserved_work]
    InForeground --> [*] : _finish() -> self.completed\n[r.foreground = None\naccount.outstanding -= len(reqs)]
```

### What Each Counter Tracks at Every Stage

| Variable | Stage 1: In `r.waiting` (Coalescing OR Ready) | Stage 2: In `r.foreground` (`load` or `compute`) | Stage 3: Completed (`self.completed`) |
| :--- | :---: | :---: | :---: |
| **Where does `ticket` live?** | `r.waiting[tid]` & `self.tickets[tid]` | `r.foreground = tid` & `self.tickets[tid]` | Deleted from `self.tickets`; outcomes in `self.completed` |
| **`account.waiting_tickets`** (Capped at 32) | **Included (`+1`)** | **Excluded (`-1` at `_grant`)** | Excluded |
| **`r.reserved_total`** | **Included (`+ticket.reserved_work`)** | **Excluded (`-ticket.reserved_work` at `_grant`)** | Excluded |
| **`account.unstarted_requests`** | **Included (`+len(requests)`)** | **Excluded (`-len(requests)` at `_grant`)** | Excluded |
| **`account.outstanding`** | **Included (`+len(requests)`)** | **Included (still in-flight!)** | **Excluded (`-len(requests)` at `_finish`)** |

---

## 4. Exact Dispatch Flow in `tick()` (Step-by-Step)

**Important Ordering Note:** We filter for **Ready `(ticket, replica)` pairs FIRST** (Step 0), and then pick the **Customer** only among customers that actually have at least one Ready pair!

```mermaid
flowchart TB
    Start(["tick() starts:\n1. backend.poll() finishes completed batches (sets r.foreground = None)\n2. Enter while True loop (one grant per iteration)"])

    Step0["Step 0: Collect Ready Pairs across Idle Replicas\npairs = [(t, r) for idle r, t in r.waiting\nif _legal(t, r) and _batch_ready(t) (128 reqs OR >= 30s)]"]

    HasPairs{"Are there any\nReady pairs?"}

    UrgentCheck{"Any idle r where\n_idle_slack(r) == 0?\n(Unready ticket about to miss deadline)"}
    Done(["Break loop:\nIdle replicas wait for 30s timer or new arrivals"])
    ForceUrgent["Pick earliest-deadline ticket on that r\n(edf_overrides += 1)"]

    Step1["Step 1: Pick Customer c* among customers in `pairs`\n• If any customer has service_gap >= 60s -> pick max(service_gap)\n• Else -> pick min(virtual_service)"]

    Step2["Step 2: Pick Best (ticket*, r*) for Customer c*\nAmong c*'s Ready pairs, pick min(_pair_cost(t, r))\nwhere pair_cost = (wait + exposed_load + 1.25) / batch_size"]

    Step3{"Step 3: Runtime EDF Check\n_safe_first(r*, ticket*) == True?\n(If ticket* runs 1st on r*, do all remaining\ntickets on r* still meet deadlines in EDF order?)"}

    EDFOverride["EDF Fallback on r*:\nReplace ticket* with min(r*.waiting, key=deadline)\n(edf_overrides += 1)"]

    Step4["Step 4: _grant(ticket*, r*)\n• Remove ticket* from r*.waiting\n• Set r*.foreground = ticket*.id\n• Pre-reserve c*.virtual_service += (1.25 + load)/weight\n• Start load (0.2s/2.0s) or compute (1.25s) on r*"]

    NextTick(["Loop back to Step 0 for remaining idle replicas;\nWhen r* finishes 1.25s later, next tick() repeats from top!"])

    Start --> Step0 --> HasPairs
    HasPairs -- "No" --> UrgentCheck
    UrgentCheck -- "No" --> Done
    UrgentCheck -- "Yes" --> ForceUrgent --> Step4
    HasPairs -- "Yes" --> Step1 --> Step2 --> Step3
    Step3 -- "Yes (Safe!)" --> Step4
    Step3 -- "No (Unsafe!)" --> EDFOverride --> Step4
    Step4 --> NextTick
```

---

## 5. How `r.certified_until` Works (O(1) Fast-Path for Batch Coalescing)

### Why Does `r.certified_until` Exist?

Imagine Replica `r` is currently idle (`r.foreground is None`) and has **2 waiting tickets** in `r.waiting` at time `now = 0s` (with `response_margin = 0` for simplicity):
- **Ticket 1 (`tail`):** `reserved_work = 2s`, `deadline = 10s`, currently has `1` request (`req_1`).
- **Ticket 2:** `reserved_work = 3s`, `deadline = 12s`.

```text
Timeline at now = 0s:
|---- Ticket 1 (2s) ----|------ Ticket 2 (3s) ------|                 |
0s                      2s                          5s               10s (T1 deadline)  12s (T2 deadline)
                         \__________________________/                  ^
                          Ticket 1 Slack = 10 - 2 = 8s                 |
                                                     \_________________|____/
                                                      Ticket 2 Slack = 12 - 5 = 7s
```

- What is the **minimum slack** across all tickets on Replica `r`?
  `min_slack = min(8s, 7s) = 7s`.
- That means even if Replica `r` sits completely idle until **`t = 7s`**, both Ticket 1 (finishes at `7 + 2 = 9s <= 10s`) and Ticket 2 (finishes at `9 + 3 = 12s <= 12s`) will **still** meet their deadlines!
- So `_feasible(r, certify=True)` saves:
  ```text
  r.certified_until = 7.0s
  ```

### What Happens When `req_2, req_3, ..., req_128` Coalesce into `Ticket 1`?

```mermaid
sequenceDiagram
    autonumber
    participant Sub as submit(request)
    participant Rep as Replica r (r.waiting)
    participant Feas as _feasible(r) [O(N log N) sort]

    Note over Rep: Initially at t=0.0s: r.certified_until = -inf (invalid)
    Sub->>Rep: req_2 arrives at t=0.1s (joins Ticket 1: size 1 -> 2)\nTicket 1 work is STILL 2s, deadline is STILL 10s!
    Sub->>Feas: now (0.1s) > certified_until (-inf) -> Run _feasible(r, certify=True)
    Feas-->>Rep: Sets r.certified_until = 7.0s (valid until t=7.0s!)

    Sub->>Rep: req_3 arrives at t=0.2s (joins Ticket 1: size 2 -> 3)\nCheck: is now (0.2s) <= r.certified_until (7.0s)? YES!
    Note over Sub,Rep: O(1) Instant Accept! (Skips _feasible sort!)

    Sub->>Rep: req_4 .. req_128 arrive at t=0.3s .. t=5.0s\nCheck: is now <= 7.0s? YES!
    Note over Sub,Rep: 125 more O(1) Instant Accepts! Zero sorting!

    Note over Rep: If a BRAND NEW Ticket 3 is added (or a ticket is removed),\nr.certified_until is reset to -inf so _feasible runs again.
```

---

## 6. On-Demand Demand-Aware LRU Eviction (`_grant` → `_start_load` → `_victim`)

**Key Rule:** We **never** evict during `submit()` (admission). Eviction is **100% lazy (on-demand)**: it only happens inside `_grant(ticket, r)` when a chosen ticket is about to run on Replica `r`, its LoRA is not in `r.resident`, and all 32 GPU slots on `r` are full.

```mermaid
flowchart TB
    Grant(["tick() calls _grant(ticket, r)"])
    HitCheck{"Is ticket.lora is None (base)\nOR ticket.lora in r.resident?"}

    Hit["Cache Hit!\n1. metrics['cache_hits'] += 1\n2. _compute(r) immediately (1.25s)\n3. Update r.resident[lora].last_use = now"]

    Miss["Cache Miss!\nSet r.phase = 'load'\nCall _start_load(r, ticket) -> _evict_for(r, demand=True)"]

    FullCheck{"Are all 32 slots full?\nlen(r.resident) == 32?"}

    PickVictim["Call _victim(r, demand=True):\n1. Exclude in-use (r.foreground) & loading (r.loading)\n2. Build queued = {t.lora for t in r.waiting.values()}\n3. Pick min(candidates, key = (lora in queued, last_use, lora))\n   -> Group A (lora NOT in queued): evict oldest last_use first!\n   -> Group B (lora IN queued): only evict if Group A is empty!"]

    EvictEngine["Evict Victim from GPU -> Host RAM:\n1. backend.evict(r.id, victim) -> engine.evict_lora(victim)\n2. Remove victim from r.resident (stays in r.warm!)\n3. metrics['evictions'] += 1"]

    StartLoad["Start Loading ticket.lora:\nbackend.start_load(r.id, ticket.lora, ticket.path)\n(0.2s if in r.warm, 2.0s if cold)"]

    LoadDone(["When load_end arrives in next tick():\n1. Add ticket.lora to r.resident & r.warm\n2. Downgrade waiting tickets for ticket.lora from 2.0s -> 0.2s\n3. Automatically call _compute(r) (1.25s)!"])

    Grant --> HitCheck
    HitCheck -- "Yes (0s load)" --> Hit
    HitCheck -- "No (Need load)" --> Miss --> FullCheck
    FullCheck -- "No (< 32 slots used)" --> StartLoad
    FullCheck -- "Yes (32/32 full)" --> PickVictim --> EvictEngine --> StartLoad
    StartLoad --> LoadDone
```

### Visual Example of `_victim(r)` Choosing Who to Evict (32/32 Slots Full)

```text
Replica r's 32 GPU Slots (r.resident):
┌─────────────────────────────────────────────────────────────────────────┐
│ LoRA_A: last_use = 10s,  has ticket in r.waiting? YES (in queued=True)  │ -> Protected!
│ LoRA_B: last_use = 50s,  has ticket in r.waiting? NO  (in queued=False) │ -> Evicted 1st! (No queued demand, oldest)
│ LoRA_C: last_use = 80s,  has ticket in r.waiting? NO  (in queued=False) │ -> Kept (newer than B)
│ LoRA_D: currently loading / running in r.foreground                     │ -> Excluded! (Never legal to evict)
└─────────────────────────────────────────────────────────────────────────┘
Result: Plain LRU would have mistakenly evicted LoRA_A (last_use=10s) even though a ticket in r.waiting
is about to use it! Demand-aware LRU evicts LoRA_B instead, saving a 0.2s warm reload!
```

---

## 7. Incremental Locality Mechanisms (`joint`, `demand`, `slru`, `prefetch`, `replication`, `marginal`, `opportunistic`)

We tested 7 incremental mechanisms on top of `Fair` (by adding one mechanism at a time in `iteration-report.md` §3 & §4) and used data to decide **what to keep in `final_config.json`, what to keep as OFF flags, and what to delete**:

```mermaid
flowchart LR
    subgraph KeptOn["KEPT & ENABLED by Default (final_config.json)"]
        J["1. joint\n(_pair_cost placement & dispatch)\nRoutes LoRAs to warm/resident replicas"]
        D["2. demand\n(Demand-aware LRU in _victim)\nCuts warm reloads by ~40% (2312 -> 1150)!"]
        J --> D
    end

    subgraph KeptOff["IMPLEMENTED, but OFF by Default (CPU & Gap Cost > Benefit)"]
        P["4. prefetch (--prefetch)\nBackground load while computing.\n2.0s load > 1.25s batch overruns & blocks\nnext customer! Gap: 94s -> 118s, CPU: 4x"]
        R["5. replication (--replication)\nMigrate 128-req batch from busy r to idle r.\nSpikes CPU 5x (15s -> 87s) & Gap -> 113s"]
    end

    subgraph Deleted["DELETED from Code (Zero / Negligible Benefit)"]
        S["3. slru (Segmented 2nd-Hit LRU)\nProduced identical results to demand\n(46,248 vs 46,248)"]
        M["6. marginal (Drop `wait` in dispatch score)\nOnly +37 reqs (+0.07%) on p80,\n0 on p50; added duplicate score code"]
        O["7. opportunistic (Early partial dispatch)\nTested as `pending` in §13: hurt both\ncompleted count & c49 max wait!"]
    end

    D -. "Tested on 82.5k & 780k traces" .-> KeptOff
    D -. "Tested on 82.5k screen traces" .-> Deleted
```

### Plain-English Dictionary of All 7 Mechanisms

| Name in Report | What It Actually Does | Measured Result & Why We Kept / Dropped It |
| :--- | :--- | :--- |
| **1. `joint`** | Uses `_pair_cost = (wait + exposed_load + 1.25) / size` at both Admission (`submit`) and Dispatch (`_choose`). | **KEPT (ON):** Establishes sticky cache affinity across the 4 replicas. |
| **2. `demand`** | In `_victim()`, prefers evicting resident LoRAs that have **no waiting tickets** in `r.waiting` (`lora not in queued`). | **KEPT (ON):** Huge win on 780k full traces—**cuts warm reloads in half (`2,312 -> 1,150`)** and adds `+7,714` completions with almost zero CPU cost (`11s -> 15s`). |
| **3. `slru`** | *Segmented / Second-Hit LRU:* protects LoRAs that have been used $\ge 2$ times from being evicted by 1-hit wonders. | **DELETED:** Produced the **exact same** completions (`46,248 / 44,441`) as `demand` because `demand` already protects active adapters! Removed to keep code simple. |
| **4. `prefetch`** | While replica `r` is computing a `1.25s` batch, use `r.loading` to background-load the LoRA for one of the next 2 tickets in `r.waiting` (and pin it for 30s). | **OFF BY DEFAULT (`prefetch: false`):** Because a cold load (`2.0s`) is longer than a compute batch (`1.25s`), when the batch finishes at `1.25s`, the loader is still busy for `0.75s`—blocking the next starving customer! Pushed `p50` max gap from `94.6s` to **`118.4s`** and CPU from `15.5s` to **`64.0s`**. |
| **5. `replication`** | If replica `target` is idle and replica `source` is busy with a full 128-request ticket waiting behind a long queue (`saved_wait > load_cost + 0.2s`), migrate (`migrate()`) that ticket to `target`. | **OFF BY DEFAULT (`replication: false`):** Re-checking both replicas' EDF ledgers on every `tick()` spiked Scheduler CPU from `15.5s` to **`87.7s`** and pushed `p50` gap to **`113.4s`** for only ~1.3% more completions. |
| **6. `marginal`** | Uses `_pair_cost` (with `wait`) at Admission, but drops `wait` at Dispatch (`(exposed_load + 1.25) / size`). | **DELETED:** Added only `+37` requests (`+0.07%`) on `p80` and `+0` on `p50`. Not worth maintaining two separate scoring formulas. |
| **7. `opportunistic`** *(later tested as `eligibility="pending"`)* | Lets an idle replica immediately run an unready (`<30s`, `<128`) partial ticket instead of waiting for `MAX_BATCH_WAIT = 30s`. | **OFF BY DEFAULT (`eligibility: "ready"`):** Locking up a non-preemptible replica for `1.25s–3.25s` on a 1-request ticket reduced total completions (`186.2k -> 183.5k`) and worsened `c49` max wait (`80.4s -> 124.5s`). |

---

## 8. Admission Control Design Options (`docs/design.md` §4)

How do we prove at admission (`submit`) that accepting a new request will not cause any accepted request to miss its `900s` deadline? We compared **3 options**:

```mermaid
flowchart TB
    subgraph Opt1["Option 1: Coarse Workload Bound (Too Pessimistic -> Rejected)"]
        C1["Rule: sum(ALL waiting tickets' work) <= min(ALL waiting tickets' deadlines)\nWhy it's bad: Forces a ticket due in 900s to finish before a ticket due in 2s!"]
    end

    subgraph Opt2["Option 2: Per-Replica EDF Prefix Check + _safe_first Fallback (SELECTED!)"]
        C2["Rule: Sort r.waiting by deadline (EDF): check prefix_sum(reserved_work) <= ticket.deadline\nAt Dispatch: Run fair/locality choice 1st ONLY if _safe_first(r, choice) passes; else run EDF head!"]
    end

    subgraph Opt3["Option 3: Global Schedule & Cache Replay (Too Complex/Slow -> Rejected)"]
        C3["Rule: On every arrival, clone all 4 replicas + LRU caches and simulate future tick() calls to t+900s.\nWhy it's bad: O(total_backlog) simulation per request melts CPU at 1,000 req/s & breaks when next request arrives!"]
    end

    Opt1 -. "Falsely rejects valid staggered deadlines" .-> Opt2
    Opt3 -. "Overkill CPU & fragile to future arrivals" .-> Opt2
```

### Concrete Numerical Example: Why Option 2 (Per-Replica EDF) Beats Option 1 (Coarse Bound)

Suppose Replica `r` is idle at `now = 0s` (`margins = 0`) and has two warm tickets (`reserved_work = 1.45s` each):
- **Ticket A:** `reserved_work = 1.45s`, `deadline = 2.0s` (urgent!).
- **Ticket B:** `reserved_work = 1.45s`, `deadline = 10.0s` (plenty of time!).

```text
1. Option 1 (Coarse Workload Bound):
   Checks if total work (1.45 + 1.45 = 2.90s) <= earliest deadline (2.0s):
   2.90s <= 2.0s  --->  FALSE! (Falsely REJECTS Ticket B, even though Ticket B isn't due until 10.0s!)

2. Option 2 (Our Per-Replica EDF Prefix Check, _feasible):
   Sorts by deadline [Ticket A, Ticket B] and checks each prefix against its OWN deadline:
   • Prefix 1 (Ticket A): 1.45s <= 2.0s   (PASS!)
   • Prefix 2 (Ticket A + B): 2.90s <= 10.0s  (PASS!)
   ---> ACCEPTS BOTH! And at runtime, _safe_first(r, Ticket B) sees running B first would finish A at 2.90s > 2.0s,
        so it automatically runs Ticket A first (at 1.45s <= 2.0s) and Ticket B second (at 2.90s <= 10.0s)!
```
