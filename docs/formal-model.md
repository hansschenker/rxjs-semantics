# Towards a formal model of RxJS behavior

**Status:** proposed semantics and research plan, not a mechanically validated model. Target RxJS 7.8.2.

## 1. Descriptions, executions and observations

Let Observable(T) be a description of a computation that may produce T values. A run is associated with a subscription identity and a context (source behavior, scheduler, environment and user functions). The description is not the same as an actual trace. A cold description can yield separate executions; a live producer may exist independently of consumers.

Notification type:

```text
N(T) = Next(T) | Complete | Error(E)
TimedEvent(T) = (time, ordinal, subscriptionId, direction, N(T))
```

Here E is the error domain (unknown in TypeScript terms). time is from a specified clock; ordinal distinguishes events at the same clock time and does not imply a universal physical clock. A trace also contains Subscribe, Unsubscribe, Schedule, CancelScheduled, ResourceAcquire, ResourceRelease and graph events where instrumented.

At one observer boundary the allowed notification language is Next* followed optionally by one Complete or Error. An explicit unsubscribe closes the boundary silently. Infinite traces or finite observations of still-live runs are permitted. A trace prefix is not necessarily a complete run.

## 2. Three projections

Let τ be a trace in a declared observation context. Define:

- V(τ): ordered next payloads at the selected boundary.
- T(τ): timestamps/order and kinds of selected notifications (including terminal events), plus timing metadata as required by the chosen observer.
- X(τ): selected lifecycle and topology events, active subscription IDs, termination causes, scheduling and resource actions.

The projections may overlap in information and lose cross-correlation. They are not a product decomposition theorem. The complete trace retains event identities, causal links and associations between values, time and execution.

Define behavior of P as the set of traces B(P, C) admitted by pipeline P in context C. The context fixes source traces, function behavior, scheduler, initial subscriptions and external actions, or defines their allowed nondeterminism. Projection equivalence means equality of sets of projected traces for all contexts in a declared class. Full trace equivalence implies each projection equivalence; the converse needs a proof and may fail when correlations are lost. A trace relation may also abstract internal node IDs and stuttering steps.

## 3. Local state machine

An operator node is a stateful transducer with labeled input and output actions. In a deterministic subset:

```text
M = (S, S0, E, A, δ, λ)
δ : S × E → S
λ : S × E → A*
step : S × E → S × A*
```

For dynamic IDs, failures, scheduling choices and reentrant callbacks, use a partial or relational transition system, or include a sufficiently detailed environment. E contains tagged incoming next/error/complete, timer due, subscription and cancellation events, and inner notifications. A contains emit, subscribe, unsubscribe, schedule, cancel, resource actions, and function invocation outcomes as appropriate. State is per node execution unless explicitly shared.

The eight policy axes (source, trigger, value, cardinality, time, concurrency, cancellation, termination) refine the three dimensions. A specification must include an initial state, a state-transition rule, an ordered action policy and ownership constraints.

### Example: pairwise

S = None | Some(T), initial None.

- On Next(x) with None: store Some(x), emit nothing.
- On Next(x) with Some(p): store Some(x), emit (p,x).
- On Complete/Error: forward terminal and close.
- On unsubscribe: close silently and release state.

This simplified local rule assumes atomic steps or an implementation that commits state before emitting into a reentrant context. The actual order of mutation and notification must be tested where reentrancy is in scope.

### Example: take(n)

For n > 0, state is remaining count plus closed flag. On next decrement count; on reaching zero arrange closure, emit nth value and complete subject to reentrant downstream cancellation and protocol guards. A scheduler/interpreter must preserve the reference implementation's observable ordering of teardown and terminal actions; do not assume a universal microstep order. take(0) completes without subscribing upstream.

## 4. Global runtime

Inspired by Zhao and Li's λrx model, the proposed global configuration is:

```text
G = (H, Q, W, clock, R)
H : subscriptionId → node state + edges + status
Q : pending delivery events / causal order
W : scheduled tasks, due times and owners
R : registered resources, teardown relations
```

H represents the dynamic graph of executing subscriptions, not the static list of operators. Q models event delivery order, W models scheduled actions, R models ownership. Some RxJS notifications propagate synchronously by nested calls; a simple FIFO queue is not an exact model. Represent synchronous delivery with an explicit continuation stack or an equivalent priority/dispatch rule and model reentrancy. Model scheduler-specific behavior separately. W and R are proposed extensions, not definitions copied from λrx.

One global step chooses a permitted external, synchronous or scheduled event, applies the relevant local rule, dispatches its actions, updates the graph/queue/resources and records an observation. A subscription action allocates an execution identity, possibly links to a shared upstream, and may produce synchronous notifications before the subscribe call returns. Unsubscription cascades along owned relationships and must not tear down an upstream still owned by a live shared subscription.

## 5. Compositional semantics

P.pipe(O1, O2) constructs an operator description; subscription instantiates the graph. Local machines interact through edges, and higher-order operators create/delete inner nodes at runtime. Sharing may cause non-tree topology; shared upstream lifetimes are governed by connector/ref-count/reset policies. Composition is not merely function composition over lists of values.

A semantics of composition must state:
1. graph construction and ownership;
2. notification routing and synchronous dispatch;
3. scheduling and cancellation;
4. terminal propagation and error interception;
5. dynamic inner subscription replacement/queuing;
6. sharing, reset, replay and reentrancy.

## 6. Equivalence and counterexamples

- map(id) versus identity: candidate observational equivalence for pure identity functions at the external boundary; not graph/instrumentation equivalence.
- delay(d) versus identity: may preserve completed value sequence but changes temporal observation; cancellation before delivery can also change values.
- share() versus identity: two overlapping consumers of a cold source can see same values and timing with different upstream execution counts.
- retry(1) versus identity: errors alter the number of subscriptions and may duplicate previously emitted values.
- switchMap versus concatMap: same structural type, different admissible execution histories and often different output values and times.

Do not infer equivalence from matching marble values alone.

## 7. Verification levels

Cω type shapes specify structure. Operator cards state policies. Mealy machines specify local transitions. A graph interpreter provides composition and execution. TLA+ can explore bounded global transition systems; LTL expresses trace obligations with explicit fairness/environment assumptions; TestScheduler and property/model-based testing can compare implementation traces.

None of these verification artifacts is automatically a proof of full RxJS behavior. See [verification roadmap](../research/verification.md).

## 8. Open formal questions

Clock and simultaneous-event semantics; reentrancy and synchronous cancellation; error ordering; arbitrary JavaScript functions; scheduler equivalences; shared resets; infinite executions and liveness; resource observability; representation of hot external producers; and whether a sufficiently expressive set of semantic observations yields a useful compositional equivalence.
