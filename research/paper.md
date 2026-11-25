# The Three Semantic Dimensions of Reactive Programming: Value, Time and Execution in RxJS

**Working research draft v0.1 — 25 November 2026**

Concept and research direction: Hans Schenker. AI-assisted drafting and synthesis: myProfessorAstra-62 (ChatGPT, GPT-6.2 by OpenAI). This is an exploratory draft, not a submission or peer-reviewed result.

## Abstract

RxJS offers a common compositional interface for transformations of values, temporal regulation and management of reactive computations. Operators with identical sequence types may produce observably different timing, subscription topology and resource lifetimes. We propose a three-dimensional framework for describing these differences: Value, Time and Execution. We outline a trace-based semantics that combines typed payloads, temporal observations and lifecycle actions, with local state-transition contracts embedded in a dynamic subscription graph. The proposal builds on established operational work on RxJS and identifies concrete verification obligations for individual operators and composed pipelines. The intended outcome is a unified vocabulary for documentation, formal reasoning, testing and reactive debugging. The framework is a hypothesis to evaluate rather than a completed formalization.

## 1. Introduction and motivation

APIs organized by operator names provide a useful reference but do not explain why the operators are necessary. A collection-oriented transformation signature captures value shape without specifying latency, event coordination or computation lifetime. For instance filter, delay, retry and share all admit T* → T*, yet differ in which concerns they address.

The central thesis is that reactive behavior can be systematically described using three complementary dimensions: what information propagates, when propagation occurs and how producing computations are executed. They overlap; we do not claim that these axes form an independent product or that every concern has an exclusive category.

## 2. Background and related work

Zhao and Li (REBLS 2022) formalize a selected RxJS subset using a small-step operational model and apply it to debugging. Their dynamic subscription graph provides an existing foundation for global execution. See [related work](related-work.md) for sources and scope. This project's intended contribution is systematic integration of value policies, temporal observations and execution policies into reusable operator contracts, with a link to local state machines and composition. Establishing novelty requires a wider literature review.

## 3. Research questions

RQ1: Can a common Value/Time/Execution vocabulary characterize all public RxJS 7.8.2 operators, with explicit exceptions or cross-cutting concerns?
RQ2: Which observations does an operator preserve, modify or introduce under specified contexts?
RQ3: How do policies interact when composed in pipelines and dynamic higher-order graphs?
RQ4: Which equivalence relations distinguish value-only, timed-notification and execution-aware behavior?
RQ5: Can the contracts be tested and, in bounded models, mechanically verified?
RQ6: Does the framework help developers predict behavior and identify bugs beyond ordinary operator descriptions?

## 4. Proposed model

Let B(P,C) denote the set of admitted traces for pipeline P in context C. A trace records notifications with time, ordering, subscription identity and lifecycle events. Projections V, T and X select the observations of interest. A local operator is described by state transitions and ordered actions; a global runtime contains a dynamic graph, delivery structure, scheduled tasks and owned resources. See [formal model](../docs/formal-model.md).

This framework distinguishes type shape, behavioral contract and operational realization. Equivalence depends on observation context and cannot be derived from type equality. The projection combination may lose correlation information. We will investigate the conditions under which equivalence is preserved by pipeline composition.

## 5. Illustrative evidence (not empirical results)

- Four flattening operators share a structural signature but implement overlap, queue, replacement and exclusion.
- A time-shifting policy may preserve a completed value sequence yet change timing and cancellation outcomes.
- Shared and independent subscriptions can emit matching values but differ in upstream resource use.
- Placement of catchError inside or outside switchMap changes the error recovery boundary.
- scan before or after share changes ownership of accumulated state.

The [composition case study](../docs/composition.md) specifies traces and assumptions. These examples demonstrate motivation, not verified coverage.

## 6. Method and evaluation plan

1. Enumerate public RxJS 7.8.2 API and state inclusion criteria (pipeable, creation, deprecated aliases, configuration variants).
2. Create multidimensional contracts and local transition tables; review against source and documentation.
3. Construct reference traces including timestamps, terminal notifications and subscription intervals.
4. Run TestScheduler, explicit reentrancy tests and bounded property/model-based generation. Record counterexamples and refine contracts.
5. Build a bounded global interpreter; compare graph and external traces under explicitly supported schedulers.
6. State and check safety/liveness obligations with environmental assumptions and fairness constraints.
7. Evaluate usefulness on live search, polling, UI state and sharing examples; a teaching study could compare prediction accuracy with API-only documentation.

## 7. Threats to validity

Version drift; scheduler assumptions; same-time ordering; synchronous reentrancy; unbounded/infinite traces; arbitrary user functions; hot external events; opaque producer cancellation; incomplete observability of resources; selection bias in operator taxonomy; limited generalization from bounded testing; conflation of implementation and specification.

The three dimensions may need refinement. Formal claims require precise semantics, evidence and clear scope. No benchmark, theorem, test run or user-study result is claimed in this draft.

## 8. Expected contributions

A coherent semantic vocabulary; reusable per-operator contracts; proposed global trace semantics relating local and composed behavior; conformance obligations and reproducible examples; a bridge from intuitive teaching to formal reasoning and debugging.

## 9. Conclusion

RxJS is not just a library for asynchronous data streams. It provides composition of value transformations, temporal relationships and execution policies. A multidimensional account of behavior is a promising organizing framework whose formal adequacy and educational utility must be evaluated.

## References

See [related work](related-work.md) and the [verification roadmap](verification.md).
