# Security Policy

## Supported Versions

Security fixes are released for the latest minor version of the current major release.

| Version | Supported |
|---------|-----------|
| 1.0.x   | Yes       |
| < 1.0   | No        |

## Reporting a Vulnerability

**Do not open a public issue for a security problem.**

Report it privately through GitHub's
[private vulnerability reporting](https://github.com/davils-com/konfetti/security/advisories/new),
or by email to development@davils.com.

Please include the affected konfetti version, module and target (JVM or Wasm), a description of the impact, the steps
to reproduce it, ideally as a Kotlin test, and any mitigation you are aware of. We acknowledge a report within three
working days and agree on a disclosure date with you.

## Threat Model

konfetti is a library: it runs in the process and with the privileges of the application that uses it. It assumes
that an attacker controls:

- the **events** an application publishes and every value derived from them, such as their **ordering keys**,
- the **headers** of publications, their names and values, including those publish interceptors add,
- **names** that come from configuration, such as the names of topics and consumers,
- the **failures** handlers throw, including their messages.

The sinks that must stay safe are logs, terminals and reports that show `toString` output, exception messages and
observations, and the availability of the bus itself.

konfetti defends against this attacker with limits that are checked before any work, a partition hash keyed with a
secret of the bus, ordered instead of hashed structures for input, containment of every callback, bounded waits for
backpressure and dead letters, and escaping at every sink. The limits are the properties of `BusLimits`; their KDoc
lists every default and every hard ceiling.

## Guarantees

- Every accepted envelope is accounted for exactly once: delivered, filtered, skipped, dead-lettered or dropped with a
  reason, by every consumer that received it, and dispatched or dropped by its topic. Nothing disappears silently.
- Envelopes with equal ordering keys reach every consumer in the order they were accepted.
- A failing handler, interceptor, observer or codec never disturbs the bus or other consumers.

## Scope

The following are **in scope**:

- input within the configured limits that makes the bus use unbounded memory or CPU, crash, or deadlock,
- an envelope that is lost, delivered twice to one consumer, or delivered out of order for its ordering key,
- ordering keys that an attacker can choose to crowd one partition of a bus with the default entropy,
- a structure that grows beyond its limit in `BusLimits`, or a limit beyond its hard ceiling,
- untrusted text that reaches a `toString`, an exception message or an observation unescaped,
- a sensitive header value that becomes visible in any text konfetti produces,
- a callback whose failure escapes into a publisher, a worker or the lifecycle of the bus.

The following are **not** vulnerabilities in konfetti:

- a vulnerability in the JDK, the JavaScript host, the Kotlin standard library, kotlinx.coroutines or Kore,
- an application that raises `BusLimits` to the hard ceilings and runs out of memory within them,
- a handler that blocks its thread, or an observer that is slow, since both run on the coroutines of the bus,
- the loss of envelopes still on their way when the process ends: konfetti holds envelopes in memory only.

## Known Limitations

- **In memory only.** konfetti is at most once across the life of a process: what is still queued when the process
  ends is lost. Within the process, every loss is counted and observed.
- **Backpressure couples the consumers of a partition.** With `OverflowPolicy.Backpressure` and no `maximumStall`, the
  slowest consumer of a partition sets the pace of every consumer of that partition and, once the buffer is full, of
  its publishers. Set a finite `maximumStall` or a dropping policy where that is not acceptable.
- **Retries hold their partition.** A consumer waits for the backoff on the worker of the failing envelope, so the
  envelopes behind it in that partition wait as well; that keeps their order.
- **Observers run inline.** Observers are called on the coroutine where the observed thing happened; a slow observer
  slows the bus. `ObservationStream` decouples collectors and tells each how many observations it missed.
- **A cancelled publisher may still have published.** When a publishing coroutine is cancelled after the topic took
  its envelope, the publication is accepted and delivered, although the caller never sees the result.
- **The principal of the bus scope.** A handler runs with the principal of its envelope; an envelope without one runs
  with whatever principal the scope of the bus carries.
- `@DavilsInternal` is a compile-time opt-in, not an access boundary, and Kotlin `internal` is public in JVM bytecode.
  `module-info.java` exports no `internal` package of `konfetti-core`.

## Verifying a Release

Releases are signed with the Davils release key `36F578E8AD2D4391E0FA6C18154C625FAD370063` and attested by the
pipeline. Verify the signature of every artifact before you use it.
