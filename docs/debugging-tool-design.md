# Designing an RxJS Debugging Tool: Subscription Graph or Mealy Machines?

## 1. The question

Two candidate foundations are on the table for an RxJS debugging tool.

1. **The subscription-graph model** from the paper behind this repository, *Semantics of RxJS* (Zhao and
   Li, REBLS '22; see [project-explanation.md](project-explanation.md), sections 1.1 and 12). Its runtime
   state is a heap of subscription nodes linked by source and child references plus an event queue. From
   it the paper derives reactive stack traces, "unexpected state" alerts, and the `safe` invariant that
   every event source is guarded by exactly one `catchError` or the root subscriber. This repository is the
   paper's implementation: a thread-like runtime whose `Subscription` objects, emitter ring buffers, and
   `Progress` tree make the graph inspectable at runtime.
2. **The Mealy-machine model** developed in the companion repositories (see section 8). Each operator is a
   six-tuple `M = (S, Σ, Γ, δ, λ, s₀)`: operator memory is the state, notifications (`next`, `error`,
   `complete`, plus timer and cancellation effects) are the alphabet, and every reachable transition row
   is a test obligation. The method is pinned to RxJS 7.8.2 and covers all 113 pipeable operators.

Which one should a debugger be built on?

## 2. Short answer

Both, at different layers. The paper's model describes a **running pipeline**: who is connected to whom,
where an event came from, what is still alive, and why a whole pipeline died. A Mealy machine describes
**one node**: given what it remembered and what arrived, what it should have done. A debugger needs the
first to navigate and the second to judge. Neither replaces the other.

## 3. What each model actually covers

| Dimension | Subscription graph (paper) | Mealy machine (companion repos) |
|---|---|---|
| Unit of description | The whole heap of nodes plus the event queue | One operator |
| State | Nodes with references, per-node fields, queued events | Operator memory `S` |
| Inputs | External events appended to the back of the queue, internal events pushed to the front | Letters of `Σ`: `next`, `error`, `complete`, timer, cancel |
| Topology | Dynamic: subscribe and unsubscribe rules add and remove nodes at runtime | Fixed per machine |
| Error propagation | A cross-node rule (R_Error) with `catchError` as the only interceptor (R_Catch) | Per-node output only |
| Cancellation | Hierarchical and cascading through the `Progress` tree | Lifecycle effects as side outputs; no cascade |
| Scheduling and reentrancy | Modelled by queue insertion position | Declared out of scope (the "reentrancy boundary") |
| Coverage | 9 operators formalised, about 100 implemented | 113 operators tabulated |
| Debugging payoff | Causality paths, reactive stack traces, graph invariants | State labels, legal-transition checks, generated tests |

## 4. Why they are the same idea at two scales

Read the paper's rules for `switchMap(r1, f, r2, b)` or `take(r, n)` as a Mealy machine and they fit
exactly:

- the fields of the subscription value are the state `S` (`n` remaining for `take`; the inner reference
  `r2` and the end flag `b` for `switchMap`);
- the input letter is the event tagged with the source it came from (`(next(v), r1)` versus
  `(next(v), r2)`);
- the outputs are the events pushed onto the queue;
- the subscribe and unsubscribe calls in the rule's premises are the lifecycle side effects.

Rule R_Take2, for instance, is one transition row: in state `take(r', 1)`, on input `(t, r')`, emit `t`
then `end`, unsubscribe `r'`, and stop. The paper therefore already contains the Mealy machines of its
nine operators, written as reduction rules. What it adds is what Mealy machines cannot express on their
own: the **composition**. Flattening operators create and destroy inner machines while the program runs, so
the set of machines is unbounded and must be represented by a heap of references. And the order in which
events reach the machines is fixed by the queue, which is precisely the reentrancy question the Mealy
method leaves open.

Conversely, the Mealy tables supply what the paper is thin on. The paper formalises nine operators and
left its "unexpected state" alerts and expression breakpoints unimplemented. The tables cover every
operator with explicit, named states, and an alert is nothing more than entering a named state.

## 5. Proposed architecture

```mermaid
flowchart TB
  cap["Capture layer: instrumented RxJS 7 (notifications in/out, subscribe/unsubscribe, inner subscriptions, timers)"]
  graph["Graph layer (paper): nodes with source/child links, last k events, lifecycle, event-queue trace"]
  mealy["Semantic layer (Mealy): reference model per node → current state, legal-transition check, named-state alerts"]
  ui["Analysis & UI: causality path, reactive stack trace, safe invariant, state inspector, replay, marble-test export"]
  cap --> graph --> mealy --> ui
  graph --> ui
```

### 5.1 Capture layer

Wrap each operator at the `pipe` boundary with a tracing operator that records notifications entering and
leaving the node, the subscribe and unsubscribe calls, and timer scheduling. Inner subscriptions created
during a node's `next` handler are attributed to that node through a "current node" context stack. Under
the default synchronous scheduler this is exact, because delivery is a nested function call. Under async
schedulers the context must be carried across the queue hop, for example with zone-style propagation; the
paper's queue model identifies exactly which events need it, namely those re-queued at the back.

### 5.2 Graph layer

Each node record mirrors the paper's subscription value plus the fields this repository already keeps:

```js
{
  id, operator,                 // 'switchMap', 'take(2)', ...
  source: [ids], child: [ids],  // the paper's r references
  events: [{dir: 'in'|'out', kind: 'next'|'error'|'complete', value, from: id, t}], // last k, as in Emitter
  latestSource: id,             // which source fired last (needed for multi-source nodes)
  lifecycle: 'active' | 'completed' | 'errored' | 'unsubscribed',
  guardedBy: id                 // nearest catchError or root, as safe.js's checkpoint
}
```

From this alone the tool can answer the paper's debugging questions: follow `source` links and
`latestSource` flags from a failure back to its origin (the dataflow path of a reactive stack trace),
evaluate the `safe` invariant, and show which inner subscriptions were alive at any moment.

### 5.3 Semantic layer

For every node, run the Mealy reference model of its operator on the node's observed input sequence. The
model's current state becomes the node's label in the inspector ("take: 1 remaining", "switchMap: outer
completed, waiting for last inner"). An observed output that the model does not predict is a divergence.
Entering a designated state raises an alert; the paper's examples all map onto states:

- `combineLatest` with a source that completed without emitting;
- `concat` whose first source completed without emitting;
- `every` whose source completed without emitting (it emits `true`);
- `switchMap` whose inner was cancelled before it emitted.

This layer is black-box over notifications. It needs no access to RxJS internals, which is its biggest
practical advantage.

### 5.4 Recording as the bridge

A captured session is a list of tuples `(node, input letter, state before, state after, outputs, time)`.
The same list is a replayable graph trace for a visualiser, raw material for generated marble tests (one
row per observed transition), and a diffable artefact for regression checks.

## 6. Practical considerations

- **Replacement versus instrumentation.** This repository's runtime *is* the graph, so the paper's
  debugging features come for free here. A tool for real applications must instrument RxJS 7, where the
  observer chain is implicit, which is the very difficulty the paper starts from. Expect the capture layer
  to be the hardest part.
- **Independent construction.** The `safe` invariant needs only the graph; Mealy conformance needs only the
  per-node traces. The two layers can be built and tested separately and composed later.
- **Gaps to keep in view.** On the paper's side: nine operators, no alerts, no breakpoints. On the Mealy
  side: reentrancy, dynamic topology, and cross-node error flow are outside the model by design.
- **Where the paper's own code helps.** `debugging/rxjs.js` already demonstrates trimmed stack traces with
  a build location, the `rxGraphTrace` operator chain, and checkpoint-based rechecking of the invariant
  as the graph grows. Those are directly reusable designs even if the runtime is not.

## 7. Recommendation and milestones

Build the graph and event history first. Without them, Mealy states have nothing to attach to.

1. **Graph and causality.** Tracing operator, graph reconstruction, last-k event buffers, and a causality
   path for the paper's type-ahead example (`test/recipes/type_ahead.html`,
   `test/error_handling/catch3.js`). Success means the tool shows why the misplaced `catchError` kills
   the pipeline.
2. **Node semantics.** Plug in the Mealy models for `take`, `switchMap`, and `combineLatest`, label nodes
   with their states, and raise the four alerts above.
3. **Invariants and export.** Implement the `safe` check over the live graph, then export a recorded
   session as marble tests.

A component-level sketch of such a tool, with the capture, graph, oracle, and analyzer layers spelled out
and a walkthrough of the paper's type-ahead bug, is in
[debugger-prototype-sketch.md](debugger-prototype-sketch.md).

## 8. Related repositories

- Mealy method and reference models: [rxjs-mealy-analysis](https://github.com/hansschenker/rxjs-mealy-analysis)
- Per-operator six-tuples for all RxJS 7 operators: [rxjs-operator-mealy-analysis](https://github.com/hansschenker/rxjs-operator-mealy-analysis)
- Transition rows to test cases: [rxjs-mealy-to-testcase](https://github.com/hansschenker/rxjs-mealy-to-testcase)
- Behavioural documentation generated from the tables: [rxjs-from-mealy-to-documentation-claude](https://github.com/hansschenker/rxjs-from-mealy-to-documentation-claude)
- Frame-by-frame marble-test visualiser: [netxpert.ch/rxjs-visualizer](https://netxpert.ch/rxjs-visualizer/)
- The paper and its implementation: [project-explanation.md](project-explanation.md)
