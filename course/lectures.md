# RxJS Deep Dive — The Semantics of Reactive Behavior

15 lectures. Each uses **Intuition → Specification → Verification**: explain what flows and why; describe state/temporal/execution rules; predict or test traces. This is a lecture syllabus with teaching notes and exercises, not yet 15 fully scripted lessons.

| Lecture | Intuition and learning objective | Specification focus | Verification exercise |
| --- | --- | --- | --- |
| 01 Computational domain | Coordinate values from sources over time; separate domain functions from reactive machinery | Observable description vs execution; push/pull duality; protocol | Compare synchronous of, timer and live event sources; count subscriptions |
| 02 Three dimensions | What flows, when, how does it run? | V/T/X profiles and trace projections | Compare filter, delay, retry, share with same structural type |
| 03 Value algebra | Projection, predication, quantification | map/filter/every, pure function assumptions | Predict values and error behavior; map identity/composition under stated equivalence |
| 04 Accumulation | State is a stream that remembers | scan/reduce/pairwise, state ownership | Verify independent subscriptions and seeded scan behavior |
| 05 Time | Arrival, event order and duration | Source, clock, scheduler, delay/observeOn | Virtual clock traces; compare next and terminal delivery |
| 06 Regulation | Too many events for consumer needs | debounce/sample/audit/throttle/buffer/window | Predict the same input under five policies; distinguish dropping from demand |
| 07 Coordination | Multiple sources and readiness | merge, zip, combineLatest, withLatestFrom, forkJoin, concat | Predict when tuples emit; test empty source and completion cases |
| 08 Lifecycle | Description vs active execution | subscribe/complete/error/unsubscribe; graph ownership | Trace teardown; take(0), takeUntil synchronous notifier |
| 09 Concurrency | New work arrives while busy | Four flatteners, queues and active inner sets | Compare equal-duration inner runs and outer completion |
| 10 Cancellation | Obsolete work and owned resources | Unsubscription vs physical abort; takeUntil placement | Verify teardown and no late downstream values with a cooperative producer |
| 11 Error and recovery | Failures have boundaries | catchError, retry, error routing | Compare inner vs outer recovery and subscription counts |
| 12 Reiteration and recursion | Restart or generate new work | repeat/retry trigger, expand recursion | Trace completion/retry and finite expand; consider stack and termination |
| 13 Sharing and replay | Many consumers, one upstream | share, shareReplay, ref count, reset, retained history | Two overlapping and one late subscriber, zero-subscriber reset |
| 14 Composition | Policies interact across dimensions | Dynamic graph, noncommutativity, case study | Reverse filter/debounce or scan/share, compare traces |
| 15 Formal verification | From intuition to executable contracts | Mealy, ROB-SN, graph transition system, trace equivalence, LTL | Specify a node, predict graph trace, derive test and counterexample |

## Common lecture pattern

Begin with a practical scenario. Draw a marble trace plus subscription intervals. Explain source and scheduler assumptions, state and control triggers. Present the concise Cω-style signature. State the three policies. End with one reference implementation example and one test that could falsify the contract.

## Capstone: type-ahead search

Students explain value normalization, quiet-period regulation, cancellation, per-request recovery, sharing and the effect of operator placement; write a TestScheduler scenario with three overlapping requests, one error and two subscribers; identify the policy defect in a misplaced catchError.

## Assessment

A learner should be able to predict not only the resulting values but also their delivery times, active subscriptions, teardown and recovery behavior. The final project is an operator contract plus a counterexample-based verification report.

Reading path: [dimensions](../docs/dimensions.md) → [contracts](../docs/operator-contracts.md) → [composition](../docs/composition.md) → [formal model](../docs/formal-model.md) → [verification](../research/verification.md).
