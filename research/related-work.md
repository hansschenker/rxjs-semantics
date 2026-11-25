# Related work — Zhao and Li, Semantics of RxJS

## Primary reference

Tian Zhao and Yonglun Li. 2022. *Semantics of RxJS*. Proceedings of the 9th ACM SIGPLAN International Workshop on Reactive and Event-Based Languages and Systems (REBLS '22), Auckland, New Zealand, 7 December 2022. DOI: [10.1145/3563837.3568340](https://doi.org/10.1145/3563837.3568340).

- [Paper PDF](https://boyang.cs.uwm.edu/publication/rebls2022.pdf)
- [Conference page](https://2022.splashcon.org/details/rebls-2022-papers/5/Semantics-of-RxJS)
- [Recorded talk](https://www.youtube.com/watch?v=GsGVbj6wKJY)
- [Authors' implementation](https://github.com/tianzhao/rxjs)
- [Annotated companion repository](https://github.com/hansschenker/rxjs-semantics-tian-zhao)
- [Companion architecture notes](https://github.com/hansschenker/rxjs-semantics-tian-zhao/blob/main/docs/project-explanation.md)
- [Companion debugger design](https://github.com/hansschenker/rxjs-semantics-tian-zhao/blob/main/docs/debugging-tool-design.md)

## What the paper establishes

The paper presents a small-step operational calculus for selected RxJS constructs. Runtime subscriptions live in a heap and carry references that encode a changing dataflow graph. Pending events carry their originating subscription references. The model separates construction of subscriptions from propagation of events and supports reasoning about reactive debugging. It illustrates failure handling in a type-ahead program and describes error-source isolation, graph-aware stack traces and state/invariant checks.

The formal subset covers fromEvent, map, take, every, combine (combineLatest), concat, switchMap, catchError and share. The accompanying implementation covers a wider subset. This is established prior art, not a contribution of the present project.

## What we found in the existing repository

The README and architecture notes explain a thread-like model implemented with cancellation scopes (Progress), asynchronous composition (AsyncM), event emitters and channels. The debugger design already advocates two scales: a graph to observe the running pipeline and a Mealy reference model to assess a node's behavior.

The documentation records limitations: default scheduler focus, incomplete option coverage and gaps between the experimental implementation and reference RxJS. It also distinguishes the paper's safety invariant from the stricter implementation in safe.js. We therefore treat it as an inspiration, not a conformance oracle. The cited documentation was reviewed; the upstream runtime was not exhaustively audited or executed.

## Proposed synthesis

| Existing work | Our extension or integration (proposed) |
| --- | --- |
| Subscription heap / graph | Explicit execution dimension, identity, ownership and topology |
| Event delivery queue | Time dimension distinguishing event order from metric time; scheduler abstraction |
| State and reduction rules | Local Mealy contracts annotated by Value, Time, Execution |
| Thread cancellation | Explicit lifecycle and resource obligations |
| Graph debugging | Runtime inspector with causal traces and policy conformance |
| Type-ahead error boundary | Comparative trace experiment for recovery placement |

The three dimensions are a pedagogical and analytical framework; the operational graph is a candidate implementation-level semantics. Neither subsumes the other automatically. The next step is to define a trace language and validate both local and composed rules under declared scheduling and function assumptions.

## Research integrity

This document is a focused comparison, not a comprehensive literature review. No novelty, priority, formal equivalence or completeness claim is established. Further review should include FRP semantics, timed and labeled transition systems, stream/dataflow semantics, denotational vs operational models, effect systems, structured concurrency and previous ReactiveX analyses.

No upstream code or paper text is copied into this project. Published authors retain their credit.
