# Multidimensional operator cards

Each contract contains stream shape, value policy, time policy, execution policy, state, initial state, ownership, lifecycle and test obligations. Notation T* is a sequence shape. Assumptions: RxJS 7.8.2, ordinary non-reentrant sources, pure total user functions unless specified. All state is per subscription except explicit sharing. These are proposed contracts to verify, not generated exhaustive operator definitions.

| Operator | Stream shape | Value policy | Time policy | Execution policy |
| --- | --- | --- | --- | --- |
| map(f) | T* → R* | f(x) for each next | no intentional scheduling | forward terminal; catch projection failure |
| filter(p) | T* → T* | retain when p(x) | no intentional scheduling | forward terminal; predicate failure errors |
| scan(f,seed) | T* → S* | emit successive state | trigger on next | allocate state per subscription |
| pairwise | T* → (T × T)* | previous + current | trigger after first next | retain one previous value |
| debounceTime(d) | T* → T* | retain latest | emit after silence | manage pending scheduled action |
| combineLatest | T1* × T2* → (T1 × T2)* | tuple of latest | emit when ready and a source emits | subscribe to inputs; coordinate terminals |
| switchMap(f) | T* → R* | project to inner, forward current inner | outer arrival replaces active inner | cancel old inner and subscribe new |
| mergeMap(f,c) | T* → R* | forward accepted inner results | interleave arrivals | bound active count, queue |
| concatMap(f) | T* → R* | forward all accepted results | sequence by inner completion | one active inner, queue |
| exhaustMap(f) | T* → R* | forward accepted results | suppress outer work while busy | one active inner, no queue |
| takeUntil(n) | T* → T* | forward until notifier next | event-triggered cutoff | subscribe notifier first; close on its next |
| timeout | T* → T* or T* → (T or R)* | forward or fallback | enforce deadline | error or replace timed-out source |
| catchError(f) | T* → (T or R)* | continue replacement results | error triggers replacement | replace failed upstream |
| retry | T* → T* | previous values not retracted | error triggers restart, optional delay | resubscribe |
| repeat | T* → T* | concatenate repeated runs | completion triggers restart, optional delay | resubscribe |
| share | T* → T* | multicast live values | no intentional replay | connector/ref-count/reset |
| shareReplay | T* → T* | retain history | replay on subscribe, optional window | shared upstream and cache |

## Card: debounceTime

Stream shape: T* → T*. State: optional pending value, latest arrival time, pending scheduled work, closed flag. Initial state: no value or work. Value policy: replace pending payload with latest arrival. Time policy: emit pending payload once silence reaches d; scheduler and tie order matter. Execution policy: arrange or update pending work, do not leave work after teardown. Ownership: one pending value/work per subscription. Lifecycle: normal completion flushes pending value then completes; error and explicit unsubscribe discard it. This describes intended observable behavior, not a claim that the RxJS implementation always cancels and creates a new timer for every next.

Obligations: burst collapse; separated emissions; completion flush; error discard; cancellation discard; same-time boundary as reference scheduler; independent subscriptions.

## Card: switchMap

State: optional active inner identity, outer-completed flag, projection index, closed status. Initial state: none, false, zero, open. Outer next: unsubscribe previous inner if present, project and subscribe new inner (the exact reentrant action ordering is an implementation obligation). Inner next: forward from current active inner. Inner complete: clear active identity; if outer is complete, complete result. Outer complete: if no inner, complete now; otherwise wait. Any uncaught active input error errors downstream and tears down owned subscriptions. Downstream cancellation tears down outer and active inner without a complete notification.

Obligations: no stale inner values after replacement; outer completion waits; projection throw tears down; synchronous inner completion; cancellation before inner response; stale resource work may still run if producer does not support abort.

## Card: shareReplay(1)

State: shared connector, replay buffer, upstream connection, ref count, terminal/reset status. Initial state: no connection, empty buffer. Value policy: replay latest cached next to later subscribers, then multicast live values. Time policy: no explicit downstream delay; replay occurs upon subscription unless other policy changes delivery. Execution policy: use configuration {bufferSize:1, refCount:true}; disconnect/reset active nonterminal connection when subscriber count reaches zero. RxJS 7.8.2 shareReplay resets after error, retains completed cache and does not reset on completion. This is not equivalent to share() or to an independently writable BehaviorSubject.

Obligations: overlapping consumers share one upstream; late consumer receives cached value; zero consumers disconnect live upstream; fresh subscription after disconnect; completion replay; error resets. Check configuration-specific behavior against pinned version.

## Card: catchError

State: upstream/replacement identity and lifecycle. On next forward. On normal completion complete. On error evaluate selector and subscribe to returned ObservableInput; if selector throws, error downstream. Explicit cancellation tears down owned active subscriptions. Replacement error propagates unless separately intercepted or the returned computation includes recovery. Returning the caught source creates a resubscription behavior, so selector behavior matters.

The location of catchError defines the recovery boundary. A catch after switchMap can end the outer search execution; a catch within the projection can keep the outer source alive.

## Card: takeUntil

Notifier next causes result completion and disposal of owned source/notifier. Notifier completion alone does not stop the source. Notifier error propagates. If the notifier emits synchronously during subscription, the source need not be subscribed at all. Explicit downstream cancellation does not emit complete.

## Template for future expansion

```text
Operator:
Stream shape:
Value policy:
Time policy:
Execution policy:
State:
Initial state:
Input events:
Transition rule:
Ordered actions:
Ownership:
Lifecycle:
Test obligations:
Assumptions and known gaps:
```

Related: [rxjs-operator-cards](https://github.com/hansschenker/rxjs-operator-cards). Integrate these fields without losing its compact original structural notation.
