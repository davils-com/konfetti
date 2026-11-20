# Events with konfetti

konfetti moves events between the parts of one process. A part publishes an event on a topic; every consumer of that
topic receives it in an envelope and handles it on its own workers. This guide explains what konfetti promises, how
to declare topics and consumers, and what happens when something fails.

## What konfetti guarantees

| Guarantee | What it means |
|---|---|
| Order per key | Envelopes of a topic with equal ordering keys reach every consumer in the order the topic accepted them, also with several workers. Without an ordering key, envelopes go to the partitions in turn and only keep their order on a topic with one partition. |
| Every envelope is accounted for | Every accepted envelope ends exactly once for every consumer that received it: delivered, filtered, skipped, dead-lettered or dropped with a reason. A topic dispatches or drops every envelope it accepted. Races, cancellations and closing the bus do not change that. |
| Nothing is lost quietly | Every dropped envelope is counted in the metrics and observed as `BusObservation.Dropped` with its `DropReason`. |
| Isolation | A failing handler, predicate, interceptor, observer, codec or ordering key never disturbs the bus, its publishers or other consumers. |
| Bounded | Every structure that grows with use is bounded by `BusLimits`; what exceeds a limit is refused, never truncated. |
| In process only | Envelopes live in memory. What is still on its way when the process ends is lost. |

## Declaring topics

A topic is named by a typed key and declared on the bus, in the builder or while the bus runs:

```kotlin
val devices = TopicKey<DeviceEvent>("inventory.devices")

val bus = eventBus(scope) {
    topicDefaults { bufferCapacity = 256 }
    topic(devices) {
        partitions = 8
        retain = 100
        orderBy { event -> event.id }
    }
}

val audits = bus.declare(TopicKey<AuditEvent>("audit.entries")).getOrThrow()
```

- `partitions` splits the topic; each partition is handled in order. A topic with several partitions but no `orderBy`
  draws a warning, because its envelopes lose their order.
- `bufferCapacity` is how many envelopes each partition holds before a publisher waits.
- `retain` keeps the last envelopes for consumers that start from `StartPosition.Retained`.
- `orderBy` derives the ordering key of an event. The key picks the partition through a hash keyed with a secret of
  the bus, so keys chosen by an attacker cannot crowd one partition.

`bus[key]` resolves a topic and fails with `TopicNotDeclaredException` or `TopicTypeMismatchException`; `declare`
returns a `Declaration` that is `Declared`, `AlreadyDeclared`, `Invalid`, `Closed` or `LimitReached`.

## Publishing

```kotlin
val trace = HeaderKey.text("trace-id")
val token = HeaderKey.text("api-token", sensitive = true)

val result = bus[devices].publish(
    DeviceEvent.Registered("r1"),
    headers = headersOf(trace("t-1"), token(secret)),
)
```

| Function | Waits for room | Inherits from the coroutine | Rejected when full |
|---|---|---|---|
| `publish` | yes | principal, cause, context | never |
| `tryPublish` | no | principal, cause, context | `Rejection.Overflow` |
| `offer` | no | nothing; it runs outside a coroutine | `Rejection.Overflow` |

A publication from inside a handler takes the envelope being handled as its cause, so a whole causal chain shares one
`correlationId`. `Attribution` decides the principal: the one of the calling coroutine, none, or an explicit one.
Publish interceptors registered with `enrich` add headers to every publication; one that fails is contained and
counted.

A publication is rejected with `Rejection.Closed` once the bus drains or closes, with `Rejection.InvalidOrderingKey`
when `orderBy` throws or returns an empty or too long key, and with `Rejection.OversizedHeaders` when its headers exceed
`BusLimits.maximumHeaders` or `BusLimits.maximumHeaderValueLength`. When a waiting publisher is cancelled, the
publication is refused, unless the topic had already taken the envelope; then it is accepted and delivered.

Headers are typed by their key and sorted by name. The value of a sensitive header travels unchanged but is shown as
`REDACTED` in every text konfetti produces.

## Consuming

```kotlin
val indexer = bus[devices].consume("inventory.indexer", owner = componentScope) {
    concurrency = 8
    mailboxCapacity = 64
    start = StartPosition.Retained
    overflow = OverflowPolicy.Backpressure
    maximumStall = 5.seconds
    handlingTimeout = 30.seconds
    onFailure = retry(attempts = 3, backoff = exponential(100.milliseconds, 5.seconds, jitter = true))
    where { envelope -> envelope.headers[trace] != null }
    handleOnly<DeviceEvent.Registered> { envelope -> index(envelope.payload) }
}.getOrThrow()
```

- `concurrency` is the number of workers. Each partition belongs to one worker, so order per key holds.
- `mailboxCapacity` is how many envelopes each worker holds.
- `overflow` decides what happens when a mailbox is full: `Backpressure` waits, `DropOldest` drops the oldest envelope
  of the mailbox, `DropLatest` drops the new one. Backpressure makes every consumer of the partition, and eventually its
  publishers, wait for the slowest consumer; a finite `maximumStall` bounds that wait and drops the envelope for the
  stuck consumer with `DropReason.Stalled` instead.
- `handlingTimeout` cancels an attempt that takes longer and fails it with a `HandlingTimeoutException`.
- `where` and `handleOnly` filter; a filtered envelope is settled, not failed.
- `owner` detaches the consumer when its scope ends.

Inside a handler, `currentEnvelope()` and `currentAttempt()` return what is being handled, and
`currentPrincipal()` of Kore returns the principal of the envelope. Consume interceptors registered with `intercept`
wrap every attempt; one that calls `proceed` a second time fails the attempt instead of running the handler twice.

`bus[devices].events()` returns the envelopes as a `Flow`; every collection attaches its own consumer, which skips what
its collector fails.

## When a handler fails

| Policy | What happens to the envelope |
|---|---|
| `FailurePolicy.Skip` | It is counted as skipped; the consumer goes on. |
| `FailurePolicy.DeadLetter` | A `DeadLetter` with the envelope, the consumer, the attempts and the failure goes to `bus.deadLetters`; the default. |
| `FailurePolicy.Stop` | The envelope and everything after it are dropped with `DropReason.Stopped`; the consumer stops with the failure. |
| `retry(attempts, backoff, then)` | The envelope is delivered again after the backoff, up to `attempts` times in all, then `then` decides. |

A retry delays the worker of the failing envelope, so the envelopes behind it in its partition wait and keep their
order. A `CancellationException` that a handler throws while its own coroutine is still active is a failure like any
other.

Dead letters wait at most the `deadLetterStall` of the bus for room; an envelope whose dead letter finds no room in time
is dropped with `DropReason.DeadLetterOverflow`, so a slow consumer of the dead letters never stops the consumers that
fail. A consumer of the dead letters cannot send its own failures back to them.

## Observing the bus

Observers registered with `observe` receive every `BusObservation`: state changes, declared topics, attached,
detached and stopped consumers, and what happens to single envelopes. They are called inline and must be fast; while
an observer's `isObserving` is false it is not called, and while no observer observes, the bus does not even build
observations. `ObservationStream` hands observations to any number of collectors on their own coroutines and tells
each collector, through `sequenced()`, how many observations it missed.

`bus.snapshot()` returns the state and the metrics of the bus, its topics and their consumers. Once a topic is drained,
`published == dispatched + dropped` holds for it, and `received == settled` for each of its consumers.

konfetti does not log. Map observations to the logger of your application in an observer.

## Limits

`BusLimits` bounds every structure: the topics of a bus, the consumers of a topic, partitions, buffers, mailboxes,
retention, attempts, headers, header values, ordering keys, handling timeouts and retry delays. Each limit has a
default and a hard ceiling that no configuration exceeds:

```kotlin
val bus = eventBus(scope) {
    limits = busLimits {
        maximumTopics = 64
        maximumHeaders = 16
        maximumOrderingKeyLength = 128
    }
}
```

## Shutting down

| Function | What it does |
|---|---|
| `drain(timeout)` | Takes nothing new, hands every accepted envelope to its consumers topic by topic and the dead letters last, waits until they are settled and closes. What is left after the timeout is dropped as abandoned. |
| `close()` | Closes at once, drops what is on its way as closed, and waits until every coroutine of the bus ended. |
| `abort()` | Closes at once without waiting, such as from code that cannot suspend. |

The bus closes as well when the scope it was built on ends. `Consumer.drain` and `Consumer.detach` do the same for one
consumer.

## Testing

`konfetti-testing` makes tests repeatable:

```kotlin
test("indexes registered devices").config(coroutineTestScope = true) {
    val recorder = EventRecorder<DeviceEvent>()
    val bus = testEventBus(backgroundScope) {
        timeSource = testScheduler.timeSource
        topic(devices)
    }
    bus[devices].consume("inventory.indexer") { handle(recorder::record) }

    bus[devices].publish(DeviceEvent.Registered("r1"))

    recorder.awaitCount(1).single().payload shouldBe DeviceEvent.Registered("r1")
}
```

`testEventBus` gives sequential ids and `SeededEntropy`, `testEnvelope` builds an envelope without a bus, and
`EventRecorder` and `RecordingObserver` record a bounded number of envelopes and observations. Never use
`SeededEntropy` in production: a predictable key lets an attacker choose ordering keys that crowd one partition.
