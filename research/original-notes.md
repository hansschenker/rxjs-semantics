# Original motivating notes — 25 November 2026

Source: Hans Schenker, uploaded file Rxjs-Dimension-Value-Time-Execution.txt, discussed in the conversation that established this repository.

## 1. Value Dimension

- Selection — discard the noise
- Projection — from raw values to Domain specific objects
- Accumulation — track change and make sense from the data

## 2. Time Dimension

- Handling Dataflow pressure — by Sampling, Auditing, ...
- Timing — delay and time Emission of data
- Synchronization/Coordination — coordinate multiple data sources

## 3. Execution Dimension

- Subscription — start the data flow
- Cancellation — stop the data flow
- Concurrency — maintain order between multiple data sources
- Error — Forward errors
- Retry — retry when error occurs
- Recovery — recover from an error

## 4. Special Behaviors

- Repeat — a loop in Rxjs
- Expand — recursion in Rxjs
- Share — share with many consumers
- Inspect — inspect the data flow

## Development of the idea

The conversation proposed the questions: What flows? When does it flow? How does it run? Value algebra, temporal semantics and execution policies describe these questions respectively. The subsequent analysis broadened concurrency to management of overlap, ordering and lifetime; distinguished downstream flow regulation from producer backpressure; integrated repeat, expand and share into the dimensions; and treated observation as a cross-cutting concern. This extension is documented separately in [dimensions](../docs/dimensions.md).

The conversation also proposed a 15-lecture course and a research paper, with operator cards, Cω-style signatures, Mealy machines and ROB-SN, trace and property-based tests, TLA+/LTL and an inspector as connected layers. These are a research agenda, not completed formal results.

Core statement: RxJS is a compositional language for describing what values flow, when they flow, and how the computations producing them are executed.
