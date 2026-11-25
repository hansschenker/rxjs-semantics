# Verification roadmap

**Status:** obligations and experimental design; no automated test suite or proof has been executed in this repository.

## Observation levels

1. Value assertions: ordered next payloads.
2. Timed notification assertions: virtual time, kind and ordering of next/complete/error.
3. Execution assertions: subscription intervals, count, inner identities, teardown and resource releases.
4. Global graph assertions: causal edges, active-node set, ownership, scheduled work and shared connections.

All levels require a declared observation boundary and context. TestScheduler gives a controlled scheduler, not a proof about all JavaScript event-loop behavior. Model-based tests should compare a specified subset and retain failing seeds/traces.

## Candidate obligations

| Obligation | Scope / qualifier |
| --- | --- |
| No downstream next after close | Observer boundary; distinguish terminal notification and explicit cancellation |
| At most one terminal | Per subscription; cancellation need not send terminal |
| Teardown at most once per registered action | Verify identity and registration semantics; functions registered separately may run separately |
| No owned active inner after downstream teardown | Cooperative subscription graph, not arbitrary external resources |
| switchMap replaces active inner | On each accepted outer next, previous inner subscription is closed |
| concatMap has at most one active inner | No reentrant model violations; queued work starts after prior completion |
| mergeMap active count ≤ configured positive finite concurrency | Work may queue, queue not necessarily bounded |
| exhaustMap does not queue ignored arrivals | Only when occupied; test projector not called for ignored values |
| Debounce flush on complete, discard on error/cancel | RxJS 7.8.2 debounceTime |
| Shared upstream execution count | share/shareReplay configuration and overlapping consumer lifetime |
| No recovery outside declared boundary | Test error before/after switchMap, then subsequent queries |
| Eventual completion of accepted work | Conditional on finite sources, completing inners, scheduler progress, no retry/repeat infinity and no cancellation |

An LTL safety sketch for an observed subscription r: G(closed(r) → X G(not next(r))) where closed is a persistent state after terminal/cancellation, and next denotes a delivery action. More precisely choose a transition-based encoding with state/action indexing to exclude same-transition next as needed. Liveness cannot be unconditional: infinite sources, never-completing inners and external failures are legal.

## Initial experiment matrix

| Experiment | Compare | Evidence |
| --- | --- | --- |
| E1 | map(id) vs identity | external values/times/termination, pure functions |
| E2 | delay vs identity | equal values in completed no-error runs; different times and early cancellation |
| E3 | four flattening policies | next trace and subscription marbles |
| E4 | inner vs outer catchError | outer continuation and recovery boundary |
| E5 | share vs independent executions | source subscription and teardown counts |
| E6 | scan/share order | accumulator ownership and late consumers |
| E7 | debounce terminal cases | pending value flush/discard |
| E8 | synchronous notifier and reentrancy | source subscription count and nested notifications |
| E9 | shareReplay reset/error/complete | cache and upstream connection lifetime |
| E10 | scheduler variants | ordering at same time, cancellation during scheduled delivery |

## Illustrative TestScheduler specification

```ts
import { TestScheduler } from 'rxjs/testing';
import { switchMap } from 'rxjs';
import { expect, it } from 'vitest';

it('switchMap cancels obsolete inner subscriptions', () => {
  const s = new TestScheduler((actual, expected) => {
    expect(actual).toEqual(expected);
  });
  s.run(({ hot, cold, expectObservable, expectSubscriptions }) => {
    const outer = hot('a-b---|');
    const a = cold('----x|');
    const b = cold('--y|');
    const output = outer.pipe(switchMap(v => v === 'a' ? a : b));
    expectObservable(output).toBe('----y-|');
    expectSubscriptions(a.subscriptions).toBe('^-!');
    expectSubscriptions(b.subscriptions).toBe('--^--!');
  });
});
```

The above is a proposed test, not executed. It checks both output and inner subscription intervals. Extend it with error, explicit cancellation and synchronous cases. Install rxjs@7.8.2 and a compatible Vitest version in a later dedicated harness.

## Milestones

M0 — Research charter, original notes, dimensions, formal proposal, related work and syllabus (this initial document set).

M1 — Fix scope and operator inventory, define trace schema, observation contexts and scheduler model.

M2 — Encode and test reference local machines for map, filter, scan, pairwise, debounceTime, takeUntil, four flatteners, catchError and shareReplay.

M3 — Dynamic graph interpreter (including synchronous dispatch, queue, schedule and ownership); conformance experiments and counterexamples.

M4 — Bounded model checking (TLA+) and trace obligations (LTL); record fairness assumptions and unmodeled cases.

M5 — Integrate operator cards and inspector; lecture examples and reproducible exercises.

M6 — Broader literature review, evaluation, revision of working paper and evidence-backed claims.

Definition of done: pinned RxJS version, executable fixtures, reproducible commands, recorded results, explicit assumptions, documented counterexamples and limitations. A matching finite test set is not a proof of full equivalence.
