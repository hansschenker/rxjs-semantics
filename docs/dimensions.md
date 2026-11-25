# The three semantic dimensions

## 1. Value — manipulation

The value dimension describes the selection, transformation, structure and history of payloads. Domain knowledge resides in supplied functions; the operator governs how those functions are applied to an Observable execution. This assumes pure, total functions for simplified algebraic reasoning; actual callbacks can throw, mutate and reenter.

| Behavior | Intention | Examples |
| --- | --- | --- |
| Projection / transformation | Apply a function to each value | map |
| Selection / predication | Allow values meeting a condition | filter, find |
| Accumulation | Produce successive states | scan |
| Aggregation | Produce a summary | reduce, min, max |
| Quantification | Determine a sequence property | every, count, isEmpty |
| Comparison | Relate successive values or streams | distinctUntilChanged, sequenceEqual |
| Pairing | Relate adjacent values | pairwise |
| Grouping | Route values by a key | groupBy |
| Collection | Assemble values into a structure | toArray, bufferCount |
| Expansion | Generate additional descriptions from results | expand |

Cω-inspired notation (sequence shape, not full semantics):

- map : (T → R) → (T* → R*)
- filter : (T → Boolean) → (T* → T*)
- scan : ((S × T) → S) → S → (T* → S*)
- pairwise : T* → (T × T)*

T* denotes zero or more values, potentially an infinite sequence in this project's notation. It does not specify the timing, notification protocol or subscription topology. The above signatures omit index arguments, overloads, this binding and possible callback failures. Seeded scan emits after each next, not an initial seed emission; use startWith for that.

## 2. Time — regulation

Time includes both metric time (clocks, durations and deadlines) and event-relative order (before, after, latest, availability and completion).

| Behavior | Intention | Examples |
| --- | --- | --- |
| Timing | Shift notification availability | delay, delayWhen |
| Sampling | Select the newest unreported value at sampling triggers | sample, sampleTime |
| Auditing | Select the latest value when a triggered duration ends | audit, auditTime |
| Throttling | Regulate how often values propagate | throttle, throttleTime |
| Debouncing | Wait for silence | debounce, debounceTime |
| Buffering | Collect values until a boundary | buffer, bufferTime |
| Windowing | Produce substreams delimited by boundaries | window, windowTime |
| Synchronization | Assemble values under availability/position constraints | combineLatest, zip |
| Coordination | Define which events trigger outputs | withLatestFrom, forkJoin, merge, concat |
| Measurement | Expose elapsed time or timestamps | timeInterval, timestamp |
| Deadline | Convert a missed deadline into an execution consequence | timeout |

Sources determine value availability. Schedulers expose a clock and scheduling mechanism where applicable. Operators decide when to schedule, retain, release, drop or react to notifications. Not every operator uses a scheduler. subscribeOn schedules subscription; observeOn schedules downstream notifications.

### Flow regulation, not automatic backpressure

Dropping, sampling, buffering, debouncing, throttling, queuing and cancellation regulate downstream traffic or work. They do not in general signal upstream demand. In particular, concatMap can accumulate an unbounded queue and sampleTime does not necessarily slow the producer.

### Example trace

Source remains active: A@0, B@80, C@160, D@480, E@560 (ms). Under ideal virtual time and default trailing/leading options:

| Operator | Output |
| --- | --- |
| delay(200) | A@200 B@280 C@360 D@680 E@760 |
| debounceTime(200) | C@360 E@760 |
| auditTime(200) | C@200 E@680 |
| throttleTime(200) | A@0 D@480 |
| sampleTime(200) | C@200 E@600 |

No terminal notification or simultaneous timer/input tie occurs in this example. Real scheduling and terminal behavior must be specified separately.

## 3. Execution — management

An Observable is a description. A subscription activates an on-demand producer or attaches to an existing live producer. The subscription owns teardown, not necessarily the external resource itself. Cancellation stops delivery and disposes registered resources; actual abort of underlying work requires a cooperating implementation.

| Behavior | Intention | Examples |
| --- | --- | --- |
| Construction | Describe computation | Observable, defer |
| Activation | Establish execution or connection | subscribe, connect |
| Cancellation | Dispose a subscription | unsubscribe, takeUntil |
| Termination | End a stream under a condition | take, first, takeWhile |
| Concurrency | Manage overlap and capacity | mergeMap |
| Serialization | Queue work | concatMap |
| Replacement | Keep latest inner subscription | switchMap |
| Exclusion | Reject new work while busy | exhaustMap |
| Reiteration | Resubscribe after completion | repeat |
| Retry | Resubscribe after error | retry |
| Recovery | Replace failed upstream | catchError |
| Recursion | Feed values into further computations | expand |
| Sharing | Multicast one active upstream | share, connect |
| Replay | Retain and deliver history | shareReplay |
| Scheduling | Schedule subscription or delivery | subscribeOn, observeOn |
| Finalization | Execute cleanup | finalize |

Concurrency is not parallelism; JavaScript can interleave many active subscriptions on a single thread.

### Higher-order policies

All four flattening operators can be summarized as (T → R*) → (T* → R*), but their subscription rules differ:

| Policy | Outer next while busy | Existing inner |
| --- | --- | --- |
| mergeMap | Start if capacity, otherwise queue | Continue |
| concatMap | Queue | Continue |
| switchMap | Subscribe to replacement | Unsubscribe |
| exhaustMap | Ignore | Continue |

For finite completion of the outer source, these operators normally wait for accepted work to finish (including queued work for mergeMap/concatMap). Error and explicit cancellation end active work according to teardown and error routing. See [contracts](operator-contracts.md).

### Lifecycle

A single subscription can deliver zero or more next values, followed by complete or error, or it can be explicitly unsubscribed without sending a terminal notification. It may also remain open indefinitely. Terminal notifications are not cancellation notifications; completion and error close the subscription and invoke teardown. Parent-child teardown and synchronous reentrancy matter.

## 4. Special and cross-cutting behavior

- repeat belongs to execution; retry differs by restart trigger.
- expand is recursive generation plus execution coordination.
- share is execution topology; shareReplay additionally retains values and may apply a replay time window.
- tap observes next/error/complete and can introduce user effects; finalize observes teardown. Neither is necessarily semantically transparent for arbitrary user callbacks.
- materialize converts next/error/complete notifications to ordinary notification values; dematerialize interprets these values. Neither turns explicit unsubscription into an error/complete notification.
- Observation, instrumentation, and verification are cross-cutting concerns.

## 5. Semantic profile (illustrative)

P = primary intent; s = secondary behavior worth exposing; – = not central under ordinary pure-function assumptions. Every operator still obeys lifecycle rules. These are analytical tags, not objective scores.

| Operator | Value | Time | Execution |
| --- | --- | --- | --- |
| map | P | – | s |
| filter | P | – | s |
| scan | P | – | s |
| delay | – | P | s |
| debounceTime | s | P | s |
| bufferTime | P | P | s |
| combineLatest | P | P | s |
| zip | P | P | s |
| switchMap | s | s | P |
| mergeMap | s | s | P |
| timeout | – | P | P |
| retry | – | s | P |
| shareReplay | s | s | P |
| takeUntil | – | s | P |
| observeOn | – | P | P |
| materialize | P | – | P |

The dimensions are not orthogonal in a strict mathematical sense. An operator's primary intention may be one dimension, but its full specification must state the other dimensions as well. For example, filtering can change the observed time sequence by removing events, even though it schedules no time delay.

## 6. Functional abstractions and effects

Functor-like projection, applicative-style combination and monadic flattening are useful composition patterns. They do not, by themselves, settle timing, cancellation, sharing or resource ownership. Lawfulness requires a precisely chosen equivalence relation and restrictions on effects. Different flattening policies need not satisfy the same laws.

Value: multiplicity and state. Time: latency and asynchronous availability. Execution: errors, iteration, concurrency, cancellation, resource lifetime and multicasting. Effects represented in types can be composed, but a type signature is not a complete behavioral specification.

## 7. Design principle

Value algebra + temporal semantics + execution policies. The domain can change; the RxJS machine stays the same. The three dimensions are the vocabulary for the machine, not its complete operational definition.
