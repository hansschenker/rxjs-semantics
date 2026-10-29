# rxjs 

This is a implementation of RxJS API using a thread-like abstraction. 
The goal is to understand the semantics of RxJS computation and provide an easier way to debug RxJS programs.

**Paper and talk**

- Paper: Tian Zhao and Yonglun Li, *Semantics of RxJS*, REBLS '22 —
  [DOI 10.1145/3563837.3568340](https://doi.org/10.1145/3563837.3568340) ·
  [open-access PDF (UWM Digital Commons)](https://dc.uwm.edu/comsci_facart/4)
- Talk: [\[REBLS\] Semantics of RxJS on YouTube](https://www.youtube.com/watch?v=GsGVbj6wKJY)
- Upstream code: [github.com/tianzhao/rxjs](https://github.com/tianzhao/rxjs)

## Main idea

The project explains RxJS by rebuilding it on top of explicit concurrency primitives, so that the meaning
of each operator becomes visible in a few lines of code instead of being buried in the real RxJS machinery.

Every observable is treated as a **thread**: a cancellable asynchronous computation that writes events into
an emitter. Operators are small concurrent programs written over a handful of textbook primitives:

- `Progress`, a tree of cancellation scopes that makes unsubscribing structured and synchronous.
- `AsyncM`, an asynchronous monad whose `timeout`, `lift`, and `all` cover all timing and interop needs.
- `Emitter`, `MVar`, and `MChannel` for passing events between threads.

With those in place, questions that are hard to answer from the RxJS source have one-line answers here.
`switchMap` is `fmap` followed by `join`, where `join` simply unsubscribes the previous inner subscription.
`zip` is one channel per source plus a loop that reads one value from each. Errors visibly skip operators
because `_listen` routes them straight to the downstream emitter unless an operator installs its own sink.
That is the "semantics" in the project name: making the cancellation, completion, error, and sharing rules
of RxJS explicit and readable.

The second goal, debuggability, follows from the first. Because the implementation already keeps an explicit
subscription graph and progress tree, the build in `debugging/` can attach a filtered stack trace and an
operator-chain trace to every error, and its `safe()` checker can walk a live pipeline and verify an
invariant such as "every producer is guarded by a `catchError`".

## Documentation

- [docs/project-explanation.md](docs/project-explanation.md): a full walkthrough of the architecture, the
  operator patterns, the debugging build, and a mapping from the paper's formal rules to the code.
- [docs/debugging-tool-design.md](docs/debugging-tool-design.md): how the paper's subscription-graph model
  and a Mealy-machine model of operators compare, and how both would fit into a debugging tool for real
  RxJS applications.
- [docs/debugger-prototype-sketch.md](docs/debugger-prototype-sketch.md): a concept-only sketch of such a
  debugger, component by component, with a walkthrough of the paper's type-ahead bug.

## Background

This code is the implementation described in Section 5 of *Semantics of RxJS* by Tian Zhao and Yonglun Li,
REBLS '22 (9th ACM SIGPLAN International Workshop on Reactive and Event-Based Languages and Systems,
Auckland, December 2022), <https://doi.org/10.1145/3563837.3568340>. The paper gives a small-step
operational semantics for a subset of RxJS, modelled as a heap holding the subscription graph plus an event
queue, derives debugging applications from it (reactive stack traces, unexpected-state alerts, and a
runtime invariant that every event source is uniquely guarded by a `catchError` or the root subscriber),
and uses this library as its testing ground. The thread abstraction, AsyncM, comes from the same authors'
SLE 2021 paper *A Concurrency Model for JavaScript with Cooperative Cancellation*.

Section 12 of the [project explanation](docs/project-explanation.md) maps the paper's formal rules onto
the code.

## Usage

For usage, please reference the test cases that can be found in the test folder. All operators were tested.
The tests are done primarily on Chrome browser. We try to emulate the semantics of the reference RxJS as close as possible.
Right now, we only support default scheduler, where the internal events are synchronously handled. 
To use async scheduler, the source code need to be modified slightly. Not all options of the RxJS operators are supported. 
