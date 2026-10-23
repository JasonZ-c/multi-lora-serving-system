# Iteration report: correctness, acceptance and customer progress

Current policy: **ready-gate eligibility, a 30s batching window, customer virtual service plus a 60s service-gap trigger, joint placement and demand-aware LRU**. Prefetch and selective replication are off. A customer joins fairness with a full or aged ticket. A small young ticket does not make that customer ready.

**The chosen comparison is [results/final-eligibility.json](../results/final-eligibility.json).** The ready gate (`results/final-virtual/`, §7) completes more requests and keeps a shorter maximum wait for the smallest customer than the pending rule (`results/eligible-virtual/`, §13), which treats a small ticket as a ready customer. Original-engine runs, identical-cohort replays, and the batch-window screens were not repeated. The core has one backlog/load pair score. The second-hit cache preference and alternative dispatch scores were removed after screening.

The 30s window is a conservative default. Earlier 30s/60s comparisons also changed the service-gap threshold, so they do not isolate batching under the latest fixed-60s policy; they remain historical evidence, not proof of an optimum. It is not a weighted aggregate score. All figures below are one seed-17 run per configuration/trace unless explicitly marked as historical calibration or correctness validation. The 2%/1% interpretation guidelines were not treated as gates.

## Evidence and reproducibility

Every machine-readable card contains a complete configuration, trace SHA-256, source SHA-256, invocation and reproducible command, per-customer offered/accepted/completed/rejected counts, rejection reasons, fixed-window goodput, drain, waits/gaps, batch fill, loads/overlap/idle time and scheduler CPU. Open the card for exact values and source identity. Initial candidate promotions preserved the original card/source identity instead of counting identical deterministic reruns as more evidence. After measured timing faults required a new common timing profile, all final comparisons were revalidated under that profile; the earlier cards remain historical.

Saved traces have 50 customers, 200 owned adapters, four replicas, and rates 250 / 1,000 / 250 requests/s. Screens use 30s / 60s / 60s (82,500 arrivals); full traces use 120s / 600s / 600s (780,000 arrivals), retaining the 900s deadline and draining accepted work. The two patterns share the same customer demand and seed. The designated ten-adapter hot set receives exactly 80% or 50% probability mass; sorting all probabilities yields 80% / 51.50% for the actual most popular ten (some adapters owned by the largest customer are similarly popular in pattern 2). The largest customer sends 40%; the smallest has 0.02% demand, about three steady requests/minute.

Short traces screen mechanisms; only full traces support sustained-backlog and sparse-customer conclusions. Per-customer counts explicitly include customers with zero accepted work. No screened or full offered-load card had a customer with zero accepted requests; this does not make high per-customer rejection acceptable or invisible.

## 1. Correctness before tuning

The replica engine's 19 contract tests passed before the serving stack was added. Further tests cover authenticated ownership, immutable registration, release CAS/idempotency/rollback, durable acceptance, request idempotency and pinned routes, single terminal outcomes, stale actor/engine fencing, context limits, ticket shapes, batch/slot bounds, legal eviction, load overlap, fairness reservation before four grants, idle-backlog reset, EDF early flush/unsafe reordering, cold work while its customer receives hot service, migration, retry, faults and restart.

Independent audits reconstruct loaded/loading/in-use adapters and compute from engine events, match exact request IDs/customer/adapter/result identities, and verify accepted/rejected/terminal conservation and each original deadline. Fairness uses actual engine batch starts. Raw original wall timestamps are retained, with monotonic logical alignment using operation anchors.

Development found and corrected clock-boundary regression in virtual events; an unsafe-prefetch sequence that could block progress on a different idle worker; accepting joins after a timing certificate expired; incorrect backlog-gap reset when work refilled during compute; and engine versus publication availability timestamps. The original-engine clock audit also exposed wall/monotonic drift, fixed without loosening audit tolerance. Aborted debugging runs are not successful experiment evidence. A constructed 200-seed virtual stress sweep covered 100,000 arrivals and 44,354 accepted requests, cache sizes 1–4, three policies, prefetch and different margins, with no failed/late/stranded work or wake spins.

## 2. Historical batching screen

Hypothesis: a bounded coalescing window improves fill and admission, at a measurable customer wait cost. Same limits and 0.25s controller/response margins are used across each matched comparison. Each paired cell is **80%-hot / 50%-hot**.

| Screen configuration | Completed after drain | Rejection | Max backlogged gap (s) |
| --- | --- | --- | --- |
| [fifo 0s](../results/screen/fifo-w0-p80.json) | 4,685 / 2,451 | 94.32% / 97.03% | 369.4 / 386.0 |
| [fifo 30s](../results/screen/fifo-w30-p80.json) | 4,920 / 2,790 | 94.04% / 96.62% | 465.5 / 465.2 |
| [fifo 60s](../results/screen/fifo-w60-p80.json) | 4,950 / 2,459 | 94.00% / 97.02% | 508.3 / 527.1 |
| [fair 0s](../results/screen/fair-w0-p80.json) | 39,873 / 40,323 | 51.67% / 51.12% | 43.2 / 41.5 |
| [fair 30s](../results/screen/fair-w30-p80.json) | 43,592 / 41,949 | 47.16% / 49.15% | 64.0 / 63.6 |
| [fair 60s](../results/screen/fair-w60-p80.json) | 47,132 / 44,190 | 42.87% / 46.44% | 94.5 / 93.0 |
| [fair 120s](../results/screen/fair-w120-p80.json) | 53,272 / 48,560 | 35.43% / 41.14% | 150.8 / 152.7 |

Decision: regrouping within customer queues is essential versus contiguous FIFO tickets. The representative window set is 0 / 30 / 60 / 120 seconds. The initial 0/30/60 results were already available when the plan was simplified; the 120s Fair screen completes the four representative values, retaining prior results as history. Promote only informative candidates; 30s and 60s were taken to full validation. Their short-trace acceptance advantage cannot establish the final fairness choice. A batching timeout makes a partial ticket eligible; it does not promise it starts at that time.

## 3. Incremental locality screen

Hypotheses: cache-aware placement can improve locality, demand-aware victims avoid needless reloads, accepted-work prefetch can hide loading, and selective full-ticket movement can reduce idle-home waiting. Change one mechanism at a time. The last two score/second-hit experiments were experimental and were removed from production after this screen.

| Screen (60s window) | Completed after drain | Max gap (s) | Smallest customer max wait (s) |
| --- | --- | --- | --- |
| [joint](../results/screen/joint-p80.json) | 46,248 / 44,441 | 94.2 / 94.4 | 117.0 / 110.2 |
| [demand](../results/screen/demand-p80.json) | 46,248 / 44,441 | 92.2 / 94.4 | 117.0 / 110.2 |
| [slru](../results/screen/slru-p80.json) | 46,248 / 44,441 | 92.2 / 94.4 | 117.0 / 110.2 |
| [prefetch](../results/screen/prefetch-p80.json) | 47,004 / 45,281 | 92.7 / 115.6 | 93.7 / 115.6 |
| [replication](../results/screen/replication-p80.json) | 48,796 / 45,772 | 94.1 / 105.6 | 110.7 / 100.2 |
| [marginal](../results/screen/marginal-p80.json) | 48,833 / 45,772 | 94.1 / 105.6 | 110.7 / 100.2 |
| [opportunistic](../results/screen/opportunistic-p80.json) | 48,155 / 45,043 | 94.0 / 92.4 | 88.7 / 100.9 |

The second-hit preference produced exactly the same headline outcomes as demand-aware LRU in both screens. Marginal dispatch added only 37 completions in pattern 1 and none in pattern 2. Remove both branches and their Config fields; retain archived code/cards for reproduction. Do not promote a victim-reload dispatch penalty without evidence of residual disruption. Opportunistic partial dispatch did not improve acceptance consistently. An earlier [virtual-only experiment](../results/screen/virtual-only-p80.json), from an earlier eligibility rule, remains historical evidence. It is not part of the current experiment plan: default Fair always combines virtual service and the customer service-gap guard, with no ON/OFF ablation.

## 4. Full locality ablations

Hypothesis: short-trace wins survive 600s sustained bursts. Run the promising mechanisms on full traces before choosing a policy. These ablations retain the 60s window and are separate from the final 30s comparison.

| Full mechanism stack | Completed | Max gap (s) | Scheduler CPU (s) | Warm reloads |
| --- | --- | --- | --- | --- |
| [joint](../results/full-locality/joint-p80.json) | 194,029 / 189,335 | 94.4 / 94.6 | 11.1 / 11.1 | 2312 / 2319 |
| [demand](../results/full-locality/demand-p80.json) | 201,743 / 195,249 | 94.4 / 94.6 | 15.5 / 19.7 | 1150 / 1280 |
| [prefetch](../results/full-locality/prefetch-p80.json) | 205,165 / 201,600 | 94.5 / 118.4 | 64.0 / 47.0 | 1179 / 1323 |
| [replication](../results/full-locality/replication-p80.json) | 207,890 / 203,007 | 93.0 / 113.4 | 87.7 / 76.2 | 1280 / 1409 |

Decision: demand-aware LRU improves accepted/completed counts in both patterns and substantially reduces warm reloads. Keep it. Prefetch and replication add acceptance, but their extra actor work and service-gap cost are consequential: the prefetch stack reaches a 118.4s gap in pattern 2; replication remains above 113s there. They stay off in the final configuration. Their options remain available to reproduce workload-dependent tradeoffs and exercise safe overlap/migration; they are not claimed as universally beneficial defaults. Residual reloads can be consequential, but the simpler demand-aware victim policy already addresses them; no unmeasured penalty is silently enabled.

## 5. Historical batching choice

Hypothesis: the 60s screen winner may be a poor fairness trade on full traffic. Compare 30s and 60s under the same core policy and retain explicit customer outcomes.

| Full configuration | Completed | Max gap (s) | Smallest customer max wait (s) |
| --- | --- | --- | --- |
| [Fair 30s](../results/full-tuning/fair-w30-p80.json) | 187,847 / 181,333 | 63.1 / 63.6 | 92.4 / 89.0 |
| [Fair 60s](../results/full-tuning/fair-w60-p80.json) | 194,066 / 187,764 | 93.7 / 93.5 | 119.2 / 91.2 |
| [Joint + demand LRU 30s](../results/full-tuning/demand-w30-p80.json) | 193,017 / 185,480 | 64.4 / 64.8 | 71.8 / 87.2 |
| [Joint + demand LRU 60s](../results/full-locality/demand-p80.json) | 201,743 / 195,249 | 94.4 / 94.6 | 100.3 / 95.3 |

Historical decision: choose **30s**. For the locality policy this reduces maximum gaps from 94.4/94.6s to 64.4/64.8s, while giving up about 4.3%/5.0% of accepted completions relative to 60s. That was the initial keep rationale. Because the threshold also changed from 60s to 90s, this is not a controlled estimate of window effects under the current fixed-60s guard. The smallest customer's requests remain fully accepted. The added 120s screen accepts more work but increases maximum gaps to about 151–153s; it reinforces the batching tradeoff without justifying another full sweep. Keep the representative 0/30/60/120 set; no intermediate or longer search is planned.

Initial full virtual Fair and selected Locality cards were promoted without deterministic reruns. Fair's earlier configuration still contained disabled experimental fields removed later; those options did not affect its policy. Final validation uses the corrected common timing profile described below. Final FIFO uses the same 30s window as Fair and Locality.

## 6. Original-engine calibration

Hypothesis: greater acceleration distorts arrivals and controller timing. Use the same screening trace/settings at speeds 10, 20 and 40, plus a separate speed-1 cold check. These are calibration runs, not final policy comparisons.

| Speed | Arrival lateness p99 (logical s) | Max lateness (s) | Completed | Rejection |
| --- | --- | --- | --- | --- |
| 10 | 0.067 | 0.297 | 46,364 | 43.80% |
| 20 | 0.455 | 1.144 | 45,747 | 44.55% |
| 40 | 10.075 | 10.253 | 43,502 | 47.27% |

Initial decision: use **speed 10**, subsequently superseded by full-backlog evidence below. Speed 40 has about ten logical seconds of arrival delay and changes acceptance; it is not a faithful default. The speed-1 cold/full-batch check completes 128 requests in about 3.28 wall seconds, matching the 2.0 + 1.25s engine path plus controller overhead.

The initial single-customer, full-base-batch scheduler saturation check achieved **366.94 fleet requests/s** over engine batch-start to final batch-end span, compared with the ideal 409.6. It includes scheduler/thread/wakeup/result overhead. The design's historical three engine-only trials had median **395.04 requests/s**; those are historical, differently scoped measurements, not repetitions of this serving benchmark or claimed current workload goodput. At the corrected final speed-5 / 1.5s-margin profile, the same saved full-base-batch trace achieved **393.14 fleet requests/s**, all 32,768 completed with zero timing-bound violations (`results/calibration/base-saturation-final.json`). This new configuration is one run, not another sample of the initial configuration. Final workload rates and original/virtual results are kept separate below.

## Original-engine validation interruption and correction

In the first original-engine validation sequence, FIFO passed, but the first full Fair/pattern-1 run had **608 accepted deadline misses**, maximum lateness **5.071s**. It was an invalid protected run, not a success or a statistical replicate. The controller's due-arrival loop starved completion publication: arrival lateness p99/max reached **16.868 / 19.767 logical seconds**, and maximum engine-done-to-publication delay was **17.294s**. This full-backlog failure was not exposed by the short calibration trace.

A deterministic costly-admission regression reproduced 163 accepted misses; bounded controller checkpoints removed those misses. The product API also checkpoints previously durable work before a new reservation, retaining the durable-before-dispatch rule. Review found a second ledger issue: loading-to-compute could replenish the whole-sequence margin. The corrected ledger freezes an absolute foreground bound and caps cached join certificates by active timing bounds. A still-active operation past its bound becomes ineligible for new commitments, with an explicit timing-bound metric; already accepted failures/misses remain counted. These controller and ledger corrections retained the original 900s SLO and initial timing margins for the next affected check.

The next affected Fair/pattern-1 check reduced the failure to **12 misses**, maximum lateness **0.602s**. The original engine still exceeded the assumed healthy timing bound: one compute took 2.593 logical seconds versus the nominal 1.25s; publication delay reached 2.251s. There were 67 timing-bound violation episodes. Arrival lateness p99/max improved to 0.339 / 2.099s. This was still an invalid protected run.

Measured pauses justify one calibration correction: **speed 5, controller margin 1.5s and response margin 1.5s**, applied identically to FIFO, Fair and Locality and to virtual/original comparisons. Each margin is 300ms of physical time at this speed, exceeding the roughly 225ms observed pause with headroom. This is an empirical host timing allowance, not an unconditional real-time guarantee. The SLO remains 900s. All six final virtual comparisons are revalidated because common admission timing changed, then the same six original comparisons run once at this profile. No policy search is expanded. This is correctness verification, not statistical replication. Supporting total-idle analysis is also bounded by the frozen execution horizon, excluding time spent inspecting results. Offline replay audit now checks exact accepted IDs against the source cohort; compact raw outcomes omit result text, whose identity checks run before export.

## 7. Final virtual validation

These are the ready-gate results, and they are the default. The pending alternative is §13. The combined decision is §14 and `results/final-eligibility.json`. These are full protected offered-load traces with the common **30s window, 60s gap threshold, 1.5s controller and response margins**, one seed-17 run per configuration/trace. The previous .25s-margin evidence above remains historical. All six runs completed every accepted request with zero failures, misses, outstanding requests, engine-contract errors or timing-bound violations. Every customer received some accepted work.

| Policy / pattern | Accepted = completed | Rejection | Burst / recovery deadline goodput (req/s) | Drain completions | Max gap (s) | Smallest customer accepted / max wait (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [fifo / 80](../results/final-virtual/fifo-p80.json) | 14,884 | 98.09% | 7.38 / 12.47 | 2,780 | 420.53 | 8/166 / 422.40 |
| [fifo / 50](../results/final-virtual/fifo-p50.json) | 9,560 | 98.77% | 4.73 / 7.82 | 1,882 | 435.52 | 7/166 / 435.52 |
| [fair / 80](../results/final-virtual/fair-p80.json) | 186,225 | 76.12% | 126.31 / 109.36 | 37,925 | 64.48 | 166/166 / 71.52 |
| [fair / 50](../results/final-virtual/fair-p50.json) | 180,121 | 76.91% | 123.31 / 103.71 | 37,995 | 63.48 | 166/166 / 80.39 |
| [locality / 80](../results/final-virtual/locality-p80.json) | 192,528 | 75.32% | 139.16 / 107.95 | 36,793 | 63.81 | 166/166 / 93.67 |
| [locality / 50](../results/final-virtual/locality-p50.json) | 184,592 | 76.33% | 134.17 / 100.11 | 37,811 | 64.79 | 166/166 / 91.22 |

Locality improves acceptance by 3.38% / 2.48% over Fair, with similar fleet maximum service gaps and 40.3% / 41.0% fewer warm reloads (1,232/1,233 versus 2,063/2,089). It costs roughly 1.5/3.9 extra scheduler CPU seconds per full trace. Its smallest customer still has all work accepted, but maximum request wait worsens from 71.52/80.39s to 93.67/91.22s; that cost is visible even though maximum customer gaps remain similar. Fair is the simpler fallback when this locality tradeoff is unattractive. Individual cold-request tails also regress: c10 in pattern 1 rises from 631.70s to 803.86s, and c18 in pattern 2 from 195.30s to 546.71s, with unchanged acceptance for those customers. Regular customer service does not imply short waits for every adapter. Locality is a tradeoff, not a dominance claim; original-engine outcomes remain the final check.

The 32-ticket limit causes all Fair/Locality rejections in these traces. The largest customer loses roughly 96% of offered requests under equal customer service. This is substantial admission loss, not hidden success. FIFO also loses conservative-capacity admissions and forms mean batches of only 3.37/2.20 requests, versus Fair's 54.80/54.12 and Locality's 55.29/54.47. Drain work is shown separately from the fixed burst/recovery windows.

## 8. Original-engine final validation

The final original-engine comparison uses speed 5 and the same common policy/limits/margins as §7. Each configuration runs once on the saved full trace; no exploratory workload runs concurrently. Timing violations are reported separately from accepted-request outcomes. Zero observed deadline misses is empirical evidence; a run with timing-bound violations is **not healthy-bound validation**.

The first FIFO/pattern-1 run completed all 15,341 accepted requests on time, but had 29 timing-bound violation episodes. Maximum controller tick duration was 85.22 logical seconds (17.04 wall seconds). A batch ended at logical 1584.51s while two results were published at 1666–1667s; the maximum publication lag was 82.68s. The longest engine operation was 5.15s, so the largest pause was in controller/publication progress. Substantial shared-host memory pressure and active paging were observed; paging is a plausible cause, not a proven attribution. These pauses are not interpreted as an engine-capacity limit. No user applications were modified. No further margin sweep or correctness claim is inferred from an on-time outcome.

All six final runs completed **754,435 accepted requests in total**, with zero failed, late or outstanding accepted work and zero engine-contract audit errors. Every customer had some accepted work. Only Fair/pattern2 had zero timing-bound violations; the other five are empirical on-time observations under timing disruption, not healthy-bound validation. Each trace offers 780,000 requests.

| Original policy / pattern | Accepted = completed | Rejection | Burst / recovery deadline goodput (req/s) | Drain completions | Max gap (s) | Smallest accepted / max wait (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [fifo / 80](../results/final-original/fifo-p80.json) | 15,341 | 98.03% | 7.46 / 13.21 | 2,751 | 437.80 | 9/166 / 635.33 |
| [fifo / 50](../results/final-original/fifo-p50.json) | 7,716 | 99.01% | 1.60 / 7.61 | 2,046 | 579.83 | 4/166 / 585.04 |
| [fair / 80](../results/final-original/fair-p80.json) | 181,419 | 76.74% | 118.59 / 107.03 | 39,577 | 65.20 | 166/166 / 60.33 |
| [fair / 50](../results/final-original/fair-p50.json) | 177,705 | 77.22% | 124.08 / 98.87 | 38,464 | 64.33 | 166/166 / 70.22 |
| [locality / 80](../results/final-original/locality-p80.json) | 191,461 | 75.45% | 140.85 / 104.26 | 37,432 | 64.98 | 166/166 / 57.30 |
| [locality / 50](../results/final-original/locality-p50.json) | 180,793 | 76.82% | 125.13 / 103.14 | 38,047 | 63.24 | 166/166 / 84.55 |

| Original policy / pattern | Arrival lateness p99 / max (logical s) | Timing-bound episodes | Max controller tick (logical s) | Scheduler CPU (s) |
| --- | ---: | ---: | ---: | ---: |
| fifo / 80 | 2.577 / 3.902 | 29 | 85.220 | 66.72 |
| fifo / 50 | 167.689 / 168.219 | 28 | 53.936 | 91.55 |
| fair / 80 | 1.966 / 8.934 | 22 | 4.531 | 31.31 |
| fair / 50 | 0.075 / 1.103 | 0 | 1.105 | 24.72 |
| locality / 80 | 0.041 / 2.032 | 1 | 1.061 | 24.64 |
| locality / 50 | 5.052 / 6.669 | 23 | 3.547 | 29.09 |

The large FIFO→Fair improvement agrees with deterministic simulation. Original Locality accepts 5.54% / 1.74% more than Fair in these single runs, but differences in arrival disruption prevent treating that magnitude as a precise policy effect. FIFO/pattern2 is especially confounded by 167.69s p99 arrival lateness. Original Locality's smallest customer wait improves in pattern1 but worsens in pattern2. The ready-gate virtual tradeoff in §7 is the default: retain joint placement and demand-aware LRU for the acceptance and warm-reload gain, keep the latency costs visible, and offer Fair as the simpler fallback. The pending comparison is §13, and the eligibility choice is §14. Prefetch/replication remain off. No extra repeats are used to optimize these noisy differences.

## 9. Fixed-60s guard update

The current policy fixes the service-gap trigger at **60s**, keeps virtual service and the guard together in default Fair, and removes ON/OFF or threshold ablations from the plan. A batch window may exceed the guard trigger; the guard only prioritizes eligible work. Six final full virtual and six original comparisons already used the fixed 60s guard. That threshold matches the current policy, and those virtual runs are the ready-gate arm chosen in §14. §13 is the pending alternative. The older window screens and full 60s ablations used different thresholds and are explicitly historical.

Only changed short-screen configurations (0s, 60s, 120s batching) were run once per trace with the guard fixed 60s. The existing 30s/60s-guard result was reused with its original source/hash and `reused_from` metadata; it is not another sample. Both patterns use the same saved seed 17 traces, .25s controller/response margins and limits.

| Batch wait; guard always 60s | Accepted = completed (80% / 50%) | Rejection | Max service gap (s) |
| --- | ---: | ---: | ---: |
| [0s](../results/fixed-guard-screen/fair-w0-p80.json) | 39,608 / 40,211 | 51.99% / 51.26% | 61.00 / 61.00 |
| [30s](../results/fixed-guard-screen/fair-w30-p80.json) | 43,592 / 41,949 | 47.16% / 49.15% | 64.04 / 63.65 |
| [60s](../results/fixed-guard-screen/fair-w60-p80.json) | 47,388 / 44,359 | 42.56% / 46.23% | 87.37 / 85.82 |
| [120s](../results/fixed-guard-screen/fair-w120-p80.json) | 53,272 / 48,560 | 35.43% / 41.14% | 143.12 / 143.15 |

All changed screens have zero accepted failures, deadline misses or outstanding work. The four representative windows demonstrate the acceptance/wait tradeoff without a threshold sweep: 30s remains a conservative default, not an exhaustive optimum. Full-scale evidence validates the selected 30s/60s-guard policy; short screens do not establish sustained-burst or sparse-customer guarantees. No additional full window optimization is needed for the default policy.

## 10. Identical accepted-work replay

These virtual execution comparisons force the same **14,884 / 9,560 FIFO-accepted IDs** from the final protected virtual traces. Admission, dispatch deadline guards and tenant caps are disabled. Excluded original arrivals are not offered or rejected replay requests. This selected cohort cannot establish general offered-load capacity or rejection. All six replays complete the exact cohort with zero failures, deadline misses or outstanding work.

| Replay policy / pattern | Completed | Burst / recovery deadline goodput (req/s) | Drain completions | Max service gap (s) | Max request wait (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| [fifo / 80](../results/replay/fifo-p80.json) | 14,884 | 7.22 / 12.07 | 3,127 | 453.46 | 506.79 |
| [fifo / 50](../results/replay/fifo-p50.json) | 9,560 | 4.50 / 7.68 | 2,118 | 479.25 | 519.03 |
| [fair / 80](../results/replay/fair-p80.json) | 14,884 | 17.11 / 5.64 | 270 | 67.11 | 207.08 |
| [fair / 50](../results/replay/fair-p50.json) | 9,560 | 10.49 / 4.19 | 121 | 64.43 | 241.86 |
| [locality / 80](../results/replay/locality-p80.json) | 14,884 | 17.06 / 5.63 | 267 | 67.11 | 219.48 |
| [locality / 50](../results/replay/locality-p50.json) | 9,560 | 10.30 / 4.19 | 121 | 64.43 | 245.12 |

Fair and Locality regroup this cohort and finish more during the burst, leaving much less drain work and shorter customer gaps than FIFO. Their outcomes are very similar on this selected sparse cohort; this is not evidence for a universal locality win. No replay admission advantage is claimed.

The first three completed cards retain their original source. An unfinished fourth replay was stopped after finding that excluded arrivals caused redundant ticks; the remaining three use a regression-tested driver that jumps between offered arrivals and all scheduler/backend wakeups. Original IDs, deadlines, timers and raw audit evidence are preserved. A 3,000-row/two-ID fixture verifies exact coalescing/load/compute times while reducing over 6,000 ticks to fewer than 30. Replay idle gaps can appear in `control_timing.max_gap`; they are not busy-controller starvation. Mixed base/LoRA diagnostics also received a JSON-key regression fix. These harness changes do not change the completed full protected policies.

## 11. Verification

The suite passes **127 tests on Python 3.10 and Python 3.12**. That includes the 19 engine contract tests, localhost HTTP auth/registration/release/inference/polling, original-engine recovery, and constructed virtual/original agreement. Independent offline audits passed all 12 final offered-load cards, 6 exact-cohort replay cards, and the final saturation card. Final protected cards show no retries or stale completions; every customer has accepted work.

Measurements ran on macOS/aarch64, with substantial shared-host memory pressure. Linux/x86 and a real GPU fleet are untested. HTTP/SQLite capacity is not claimed by the scheduler-only workload benchmark. A clean-tree check ran the same 127 tests, the lifecycle demo (pinned v1 completed after promotion and rollback, `slo_violation=false`), and a 30-request seed-17 trace with a passing raw audit. Production configurations keep the 60s guard.

## 12. Work-conserving eligibility update

A later eligibility change treats pending requests plus a legal available worker or move as eligible, including one-request young partials. The former readiness filter excluded those customers before fairness selection and could intentionally idle replicas. That filter and the opt-in `opportunistic` switch are removed. All legal pending tickets now participate in customer selection; virtual service is reserved before parallel grants and the fixed 60s guard includes young partials. Full or timed-out tickets receive preference only within the selected customer; if none exist, its best legal young partial runs immediately. The existing preferred-dispatch/EDF fallback still checks accepted commitments.

Regression tests cover immediate base/cold singleton grants with 120s batching, young partials winning virtual-service and service-gap selection against full batches, blocked overdue customers, preference within one customer, four parallel fair grants, and EDF overriding an unsafe young-partial choice. A constructed original-engine integration test checks immediate cold-singleton dispatch. Existing strict-wait assertions and idle accounting were updated to the new behavior. These correctness tests passed before the workload rerun: **126 tests on Python 3.12 and Python 3.10**. The archived tables above stay historical. The six full virtual offered-load runs were then repeated; §13 records them.

## 13. Work-conserving virtual comparison

This is the pending-eligibility arm, kept for comparison. It is not the default. These six runs let any customer with pending work and a legal available worker compete, including a single young request. Fairness and the fixed 60s guard run before the within-customer full-or-aged preference. The traces, 30s preference, 60s guard, 1.5s margins, and FIFO / Fair / Locality settings match §7. One seed-17 run per configuration. All six cards share source `2979015bb0e4e8be4a08c8b164565444e4137a37875c40df80a88c40e361cb35`. Mean Fair batch size stayed near 53, so the 30s preference was not reopened.

Every accepted request completed, with zero failures, deadline misses, outstanding requests, engine-contract errors, or timing-bound violations. Every customer received some accepted work. Queued-idle time was zero on every card.

| Policy / pattern | Accepted = completed | Rejection | Burst / recovery deadline goodput (req/s) | Drain completions | Max gap (s) | Smallest customer accepted / max wait (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [fifo / 80](../results/eligible-virtual/fifo-p80.json) | 14,868 | 98.09% | 6.99 / 12.93 | 2,678 | 366.49 | 9/166 / 448.55 |
| [fifo / 50](../results/eligible-virtual/fifo-p50.json) | 9,862 | 98.74% | 4.70 / 8.09 | 1,999 | 420.70 | 6/166 / 421.13 |
| [fair / 80](../results/eligible-virtual/fair-p80.json) | 183,559 | 76.47% | 128.29 / 109.29 | 38,652 | 62.99 | 166/166 / 94.43 |
| [fair / 50](../results/eligible-virtual/fair-p50.json) | 178,272 | 77.14% | 120.87 / 106.96 | 38,215 | 61.74 | 166/166 / 124.48 |
| [locality / 80](../results/eligible-virtual/locality-p80.json) | 191,413 | 75.46% | 141.75 / 109.68 | 36,582 | 61.74 | 166/166 / 104.05 |
| [locality / 50](../results/eligible-virtual/locality-p50.json) | 183,341 | 76.49% | 134.59 / 100.39 | 38,770 | 61.96 | 166/166 / 94.14 |

Against the ready-gate §7 cards, Fair accepts 1.43% / 1.03% fewer requests. Mean batch size moves from 54.80/54.12 to 53.36/52.68, and the median from 27/26 to 25/24. Fleet maximum service gaps stay next to the 60s trigger. The smallest customer still has all 166 requests accepted; its maximum request wait rises from 71.52/80.39s to 94.43/124.48s.

Locality accepts 4.28% / 2.84% more than Fair and performs 38.1% / 36.3% fewer warm reloads (1,208/1,284 versus 1,952/2,016). Its mean batches are 54.47/53.00. Maximum gaps remain similar. The smallest customer's maximum wait is 104.05s on pattern 1 and 94.14s on pattern 2, so the extra wait appears on pattern 1. Scheduler CPU is 12.68/13.40s for Fair and 15.18/13.51s for Locality. c10 in pattern 1 waits 650.31s under Fair and 893.60s under Locality, with acceptance unchanged at 7,108. c18 in pattern 2 waits 278.61s under Fair and 293.35s under Locality, with acceptance unchanged at 2,283.

FIFO stays near 98% rejection. Accepted counts move from 14,884/9,560 to 14,868/9,862, and mean batches are 3.32/2.24. Fair and Locality rejections are still entirely the 32-ticket tenant cap. The largest customer loses about 97% of offered requests under equal customer service. Drain stays separate from the fixed burst and recovery windows.

Retain joint placement and demand-aware LRU for the acceptance and warm-reload gain. Fair remains the simpler fallback. Prefetch and replication stay off. No original-engine rerun, replay, or 0/30/60/120 sweep was added.

## 14. Eligibility decision

Both arms are combined in [results/final-eligibility.json](../results/final-eligibility.json). The default is the **ready** gate from §7. Marking a small young ticket as a ready customer, the §13 pending rule, accepts 1.43% / 1.03% fewer Fair requests and moves the smallest customer's maximum wait from 71.52/80.39s to 94.43/124.48s. Locality shows the same direction: fewer completions and a longer pattern-1 small-customer wait. The pending rule removes about 38 replica-seconds of coalescing idle on Fair/80%, which does not repay the lost throughput. `Config.eligibility` defaults to `ready`; `pending` remains selectable.
