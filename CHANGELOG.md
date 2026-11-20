# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.0.0 - 2026-11-20

The first version of konfetti: the event library of Davils as an open source library under the Apache License 2.0,
for Kotlin on the JVM and on `wasmJs`, built with Kreate 5.3.0 on Kore 2.3.1 and published to
`https://repo.davils.com`. The code comes from the event library of an internal Davils project and is hardened for
critical infrastructure on the way.

### Added

- **The build** of konfetti with Kreate 5.3.0: one convention plugin per concern, strict dependency locking and
  verification, Detekt with every rule over main and test sources, Kover at 97 %, the API and semver check, Trivy,
  the standards audit and the assurance gate.
- **Typed topics** with partitions that keep the order of an ordering key, bounded buffers, retention for consumers
  that start from the retained envelopes, and declaration in the builder or at run time.
- **Publishing** that waits for room, that never waits, and from outside a coroutine, with attribution to a
  principal, causation and correlation, and publish interceptors that enrich headers.
- **Consumers** with several workers, bounded mailboxes, the overflow policies backpressure, drop oldest and drop
  latest, a longest stall for backpressure, predicates, consume interceptors, handling timeouts and owner scopes.
- **Failure policies**: skip, stop, dead letter and retries with fixed or exponential backoff and full jitter.
- **Dead letters** that never block a failing consumer longer than the dead-letter stall of the bus.
- **Typed headers** with built-in codecs and sensitive values.
- **Observations** for observers, an `ObservationStream` that tells each collector what it missed, metrics and
  snapshots of the bus, its topics and their consumers.
- **The lifecycle**: drain with a timeout, close, abort without waiting, and the end of the owning scope.
- **`BusLimits`** with a hard ceiling for every structure that grows with use.
- **konfetti-testing**: a deterministic test bus, test envelopes, seeded entropy and bounded recorders.

### Security

- Every accepted envelope is accounted for exactly once, also when a channel is cancelled while a receiver takes an
  element, when a publisher is cancelled while it waits, and when the bus closes during a drain.
- Ordering keys pick partitions through SipHash-2-4 keyed with a secret of the bus from secure randomness.
- Headers are bounded in count and length and kept sorted instead of hashed; the topics of a bus are bounded and
  ordered; the consumers of a topic are bounded.
- Ordering keys that throw, are empty or too long reject the publication instead of failing the publisher.
- Handlers, predicates, interceptors, observers, codecs and the entropy run contained; an interceptor that proceeds
  twice fails the attempt instead of running the handler twice.
- Every text from a caller is escaped and bounded wherever it is shown; payloads and failure messages are never shown,
  and sensitive header values are replaced by `REDACTED`.
- The runtime lives in packages the Java module does not export.
