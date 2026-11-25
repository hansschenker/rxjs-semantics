# Composition of semantic policies

## Case study: reactive live search

Intuition: queries flow from user events; results arrive after variable latency. Normalize and select queries, wait for silence, keep only the latest accepted request, recover per request, then share results.

```ts
const results$ = query$.pipe(
  map(query => query.trim()),
  distinctUntilChanged(),
  debounceTime(300, asyncScheduler),
  switchMap(query =>
    defer(() => search(query)).pipe(
      catchError(() => of([]))
    )
  ),
  shareReplay({ bufferSize: 1, refCount: true })
);
```

Assume search returns Observable<Result[]> with cooperative teardown for cancellation, query$ is a source with declared hot/cold behavior, and imports are from RxJS 7.8.2. This is an illustrative pipeline, not a standalone app. Treating failure as [] is a deliberate simplified domain choice; a discriminated result type can distinguish failed and empty searches.

| Stage | Dimension | Policy |
| --- | --- | --- |
| map, distinctUntilChanged | Value | Normalize and deduplicate |
| debounceTime | Time | Wait for quiet period |
| switchMap | Execution | Replace previous active inner |
| inner catchError | Execution | Recover within one request |
| shareReplay | Execution + Value | One overlapping upstream; remember latest result |

The pipeline is a description until connected. If query$ is live, its producer may already exist. A new raw keystroke does not immediately cancel an old search: it must survive normalization/deduplication and reach switchMap after debounce. Cancellation of delivery is not guaranteed physical network abort. If immediate cancellation on typing is desired, change the topology, e.g. switchMap before a per-query timer/deferred search.

## Recovery topology: inspiration from Zhao and Li

```ts
// A failed inner ends the original outer execution.
const outerRecovery$ = query$.pipe(
  switchMap(q => defer(() => search(q))),
  catchError(() => of([]))
);

// A failed inner is replaced; the outer stays subscribed.
const innerRecovery$ = query$.pipe(
  switchMap(q => defer(() => search(q)).pipe(
    catchError(() => of([]))
  ))
);
```

This distinction, illustrated by the paper's type-ahead example, makes graph position part of the semantics. Both pipelines can emit the same fallback once, but differ when another query arrives after failure. This is an execution-policy difference with subsequent value/time consequences.

## Concurrency trace

Outer A@0, B@2, C@4, completes@5. Each inner is on-demand, emits its label exactly 3 time units after subscription, then completes. No errors or cancellation except policy-driven replacement; assume ideal virtual scheduler. No equal-time input/inner completion ties for these three outer arrivals.

| Policy | Inner subscription intervals | Next output |
| --- | --- | --- |
| mergeMap (unbounded) | A[0,3], B[2,5], C[4,7] | A@3 B@5 C@7 |
| concatMap | A[0,3], B[3,6], C[6,9] | A@3 B@6 C@9 |
| switchMap | A[0,2] cancel, B[2,4] cancel, C[4,7] | C@7 |
| exhaustMap | A[0,3], C[4,7]; B ignored | A@3 C@7 |

Outer complete at 5 may coincide with B completion under mergeMap; their order does not change the listed next values here. All complete after accepted work finishes (merge/switch/exhaust at 7, concat at 9). Each interval represents subscription lifetime, not necessarily physical work.

## Shared reactive state

```ts
const model$ = actions$.pipe(
  scan(update, initialModel),
  startWith(initialModel),
  shareReplay({ bufferSize: 1, refCount: true })
);
```

Value: each action transforms previous state into next state. Time: update triggered by action arrival. Execution: overlapping consumers share one reducer execution and receive current state. With refCount true a live nonterminal upstream can disconnect and reset state after all consumers leave; this is not perpetual application-state ownership. Use an explicitly owned connection or other lifecycle policy when persistence across zero subscribers is required.

## Noncommutativity and interaction

- filter(p) then debounceTime(d) differs from debounceTime(d) then filter(p): rejected arrivals may or may not reset the temporal gate.
- share then scan allocates accumulator per downstream subscriber; scan then share distributes one accumulator execution.
- retry inside switchMap restarts an individual request; retry outside can restart the entire upstream subscription.
- observeOn can move notifications across a scheduling boundary and affect ordering relative to cancellation.
- takeUntil placement changes which subscriptions it owns and which inner computations are torn down.

The semantics of a pipeline is therefore not the sum of operator descriptions considered in isolation. It is the interaction of local policies under a runtime graph and scheduler.
