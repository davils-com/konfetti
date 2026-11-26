# konfetti

konfetti is the event library of Davils: an in-process event bus for Kotlin on the JVM and on WebAssembly (`wasmJs`),
built on [Kore](https://github.com/davils-com/kore) for use in critical infrastructure. Typed topics split their
envelopes over partitions that keep the order of a key, consumers receive them through bounded mailboxes, failures
are retried, skipped, dead-lettered or stop a consumer, and every accepted envelope is accounted for exactly once. It
is published under the [Apache License 2.0](LICENSE).

| Module | Artifact | What it holds |
|---|---|---|
| `konfetti-core` | `com.davils:konfetti-core` | The bus: topics, partitions, publishing, consumers, mailboxes, retries and backoff, dead letters, headers, interceptors, limits, observations, snapshots and the drain and close lifecycle |
| `konfetti-testing` | `com.davils:konfetti-testing` | A deterministic test bus, envelopes built by hand, seeded entropy and bounded recorders of envelopes and observations |

konfetti is a Kotlin Multiplatform library for `jvm` and `wasmJs`, written for Kotlin 2.4 or newer. On the JVM it
requires JDK 25; `konfetti-core` is the named Java module `com.davils.konfetti.core`. On Wasm it is tested under
Node.js. The bus runs on the coroutine scope you give it and never starts a thread of its own.

## Installation

konfetti is published to the Davils repository, not to Maven Central. Declare it for the `com.davils` group only:

```kotlin
// settings.gradle.kts
dependencyResolutionManagement {
    repositories {
        exclusiveContent {
            forRepository { maven("https://repo.davils.com/releases") }
            filter { includeGroupAndSubgroups("com.davils") }
        }
        mavenCentral()
    }
}
```

```kotlin
// build.gradle.kts
dependencies {
    implementation("com.davils:konfetti-core:1.1.0")
    testImplementation("com.davils:konfetti-testing:1.1.0")
}
```

A Maven build names the JVM artifacts directly, such as `com.davils:konfetti-core-jvm`.

## A short tour

```kotlin
sealed interface DeviceEvent : Event {
    val id: String

    data class Registered(override val id: String) : DeviceEvent
}

val devices = TopicKey<DeviceEvent>("inventory.devices")

val bus = eventBus(scope) {
    topic(devices) {
        partitions = 8
        orderBy { event -> event.id }
    }
}

bus[devices].consume("inventory.indexer") {
    concurrency = 8
    onFailure = retry(attempts = 3, backoff = exponential(100.milliseconds, 5.seconds, jitter = true))
    handle { envelope -> index(envelope.payload) }
}.getOrThrow()

when (val result = bus[devices].publish(DeviceEvent.Registered("r1"))) {
    is PublishResult.Published -> result.envelope.id
    is PublishResult.Rejected -> result.reason
}

bus.drain(30.seconds)
```

[docs/events.md](docs/events.md) explains the guarantees, the settings and every failure mode.

## Security

konfetti assumes that events, ordering keys, headers and the text of failures may come from an attacker. Every
structure is bounded by `BusLimits`, ordering keys pick partitions through a keyed hash, foreign code runs contained,
and text from callers is escaped wherever it is shown. [SECURITY.md](SECURITY.md) describes the threat model and how
to report a vulnerability.

## Building

```bash
./gradlew check
```

It needs JDK 25 and network access. [AGENTS.md](AGENTS.md) holds the rules every change follows and
[CONTRIBUTING.md](CONTRIBUTING.md) how a change gets in.

## License

Copyright 2026 Davils. Licensed under the [Apache License, Version 2.0](LICENSE).
