# AGENTS.md

Binding rules for every agent and every person working in this repository. konfetti is the event library of Davils:
an in-process event bus with typed topics, ordered partitions, bounded mailboxes, retries, dead letters and an exact
account of every envelope, for Kotlin on the JVM and on WebAssembly (`wasmJs`), built on
[Kore](https://github.com/davils-com/kore). It is published under the Apache License 2.0 and is built for critical
infrastructure, with security and correctness first.

## Repository

| Module | Artifact | Purpose |
|---|---|---|
| `konfetti-core` | `com.davils:konfetti-core` | The bus: topics, partitions, publishing, consumers, mailboxes, delivery, failure policies, dead letters, headers, limits, observations, snapshots and the lifecycle |
| `konfetti-testing` | `com.davils:konfetti-testing` | Test support: a deterministic bus, envelopes built by hand, seeded entropy and bounded recorders |
| `build-logic` | - | One convention plugin per concern, composed by `konfetti.kotlin-library` for the modules and `konfetti.root` for the root project; they hold nothing but `plugins { }` and `kreate { }` |

`konfetti-core` builds on `com.davils:kore-core`, `konfetti-testing` on `konfetti-core` and `com.davils:kore-testing`.
The code is ported from the event library of an internal Davils project and hardened on the way.

The root package of a module is `com.davils.konfetti.<module without konfetti->`, such as `com.davils.konfetti.core`,
and its Java module has the same name. `konfetti-core` declares it in `src/jvmMain/java/module-info.java` and exports
only packages with public API, never an `internal` package. `konfetti-testing` has no `module-info.java`, because
`kore-testing` is an automatic module; it has the `Automatic-Module-Name` `com.davils.konfetti.testing`.

## Targets

Every module is a Kotlin Multiplatform library with two targets: `jvm` and `wasmJs`. The Wasm tests run under Node.js.

- Code lives in `src/commonMain/kotlin`. What only the JVM offers, such as `LongAdder` and `ReentrantLock`, hides behind
  an `internal expect fun` in `<Role>.kt` with its `actual` in `<Role>.jvm.kt` and `<Role>.wasmJs.kt`, and a parity
  test proves that both targets behave the same. konfetti declares no `expect` or `actual` class: they are a Beta
  language feature.
- The bus never chooses a dispatcher and never starts a thread: it runs on the scope its caller hands to `eventBus`.

## Verify

```bash
./gradlew check
```

Nothing is done until it is green. `check` compiles with warnings as errors (Kotlin with progressive mode and extra
warnings, javac with `-Xlint:all -Werror`), runs Detekt with type resolution, the Kreate rule set and `allRules` over
the main and the test sources, the tests on the JVM and on Wasm, Kover verification at 97 % of lines, branches and
instructions, the API and semver check, the license header check, the changelog check, every Trivy scan, the
standards audit and the Kreate assurance gate, which blocks on every finding. The gate reads OSV.dev and deps.dev and
Trivy downloads its database, so `check` needs the network. The build runs on JDK 25; toolchains are never downloaded.

## Code rules

Enforced by Detekt with the Kreate rule set, the `libraries` rule set, `allRules` and warnings as errors, without a
baseline, for main and test sources alike. Build scripts and `build-logic` follow the same rules.

- **License header first.** Every `.kt`, `.kts`, `.java`, `.properties` and YAML file starts with the Apache 2.0
  header, byte for byte as in the existing files.
- **No comments.** No `//`, no `/* */` other than the license header. Names, types and tests carry the intent.
- **KDoc on public API only**, with `@param`, `@return`, `@throws` and `@since`. A new declaration gets the version it
  first ships in; existing `@since` values never change. KDoc states behaviour on invalid input, the limits that apply,
  thread safety and every security consideration.
- **No `else`.** Return early; a `when` over a sealed type or an enum is exhaustive without `else ->`. At most two
  `return` statements per function.
- **At most two chained calls.** Name intermediate results and computed conditions.
- **One top-level type per file**, named after it; a file of functions is named after its role.
- **No package for a single file**, except the root package of a module and `internal` packages.
- **No `!!`, no `lateinit`.**
- **Explicit API.** Everything is `internal` unless a consumer needs it.
- **Names.** No type, file, function or property carries the name of the library as a prefix: `EventBus`, never
  `KonfettiEventBus`. The ids of the Gradle plugins keep their `konfetti.` prefix. konfetti uses the annotations of
  Kore, such as `DavilsInternal` and `DavilsDsl`, and declares none of its own.
- **Immutable values.** Public types are immutable. No public `data class`: a public value writes `equals`,
  `hashCode` and `toString` by hand; `value class` stays allowed.
- **Kotlin first.** Atomics come from `kotlin.concurrent.atomics` through a local `@OptIn(ExperimentalAtomicApi::class)`;
  experimental API never appears in a public signature.
- **Nothing global.** No mutable singletons, no static configuration, no global caches, no threads or timers of the
  library's own.
- **No logging.** konfetti has no logger, no log observer and no log types. Telemetry runs only through `BusObserver`
  and `ObservationStream`; an application maps observations to its own logs.

## Event rules

- **Every envelope is accounted for exactly once.** Every accepted envelope ends, for every consumer that received it,
  in exactly one of delivered, filtered, skipped, dead-lettered or dropped with a `DropReason`, and before any consumer
  received it, in dispatched or dropped. The identities `published = dispatched + dropped` for a topic and
  `received = settled` for a drained consumer hold under every race, cancellation and close. A change that touches
  publishing, partitions, mailboxes, delivery or the lifecycle ships with a test of these identities, and the race and
  stress tests in `src/jvmUnitTest` stay green when run many times in a row.
- **Every handoff has an owner.** A channel does not reliably hand every buffered element to its undelivered callback
  when it is cancelled while a receiver takes one, so partitions and mailboxes track what they hold and settle what is
  left after a cancel themselves. Handoffs move through atomic states with compare-and-set and exchange; a state is
  never read and written in two separate steps.
- **Foreign code is contained.** Handlers, predicates, interceptors, observers, `isObserving`, codecs, `orderBy` and
  the entropy run inside `contained` or `attempted` of Kore. A failure of a callback never escapes into a publisher, a
  worker or the lifecycle; it becomes a failed attempt, a rejection, a counted failure or a fallback.
- **Nothing blocks without a bound the caller chose.** Backpressure waits for room as long as `maximumStall` allows;
  dead letters wait at most the dead-letter stall of the bus. Retries keep the order of a partition by delaying its
  worker.
- **Every structure that grows has a limit.** A new structure that grows with use gets a property in `BusLimits` with a
  hard ceiling, and the bus checks it before the work.

## Security rules

Every input of konfetti may come from an attacker: events and their ordering keys, header names and values, consumer
and topic names that come from configuration, and the text of failures. konfetti is built so that a mistake of a
caller fails closed.

- **Limits before work.** Topics, consumers, partitions, buffers, mailboxes, retention, attempts, headers, header
  values, ordering keys, handling timeouts and retry delays are bounded by `BusLimits`, whose hard ceilings no
  configuration exceeds. A publication beyond a limit is rejected with a `Rejection`, a declaration or a consumption
  beyond a limit is refused with a result, and nothing is truncated silently.
- **No input decides the complexity.** Ordering keys reach partitions through SipHash-2-4 keyed with a secret of the
  bus, so chosen keys cannot crowd one partition. Headers are kept sorted and found by binary search; the directory of
  topics is ordered; no structure keyed by input relies on hash codes alone.
- **Escape untrusted text exactly once, at the sink.** Every `toString` and exception message that shows text from a
  caller writes it through `SafeText` of Kore. Payloads are never shown, only their type; failures are shown by type,
  never by message.
- **Secrets never leak.** A sensitive header value is replaced by `REDACTED` wherever headers are shown.
- **Randomness is secure by default.** The jitter of backoffs and the key of the partition hash come from the
  `Entropy` of the bus, which defaults to `SecureEntropy` of Kore. `SeededEntropy` exists for tests only.
- **Hide what Java must not reach.** The runtime lives in `internal` packages that `module-info.java` never exports.
- **Every sink has an injection test** with text built from `HostileFragments` of `kore-testing`, and every limit has a
  test at the limit and one past it.

## Tests

- Kotest `FunSpec` only, Kotest assertions only, no mocking library.
- Tests live in `src/commonUnitTest/kotlin`, which runs on the JVM and on Wasm, in the package of the code they test.
  Tests that need real threads, such as the race and stress tests, live in `src/jvmUnitTest/kotlin`. A `fixture` folder
  holds at least two files.
- Virtual time through the test scheduler of Kotest; `testEventBus` of `konfetti-testing` makes ids and entropy
  deterministic.
- Every change ships with its tests; a fixed weakness ships with a test that would have caught it.

## Build

- Every module applies exactly one plugin, `id("konfetti.kotlin-library")`, and declares nothing but its description
  and its dependencies.
- `build-logic` holds no Kotlin classes, no tasks and no Gradle API calls beyond reading the version catalog. Every
  plugin configures one concern and applies `konfetti.base`:

  | Plugin | Configures |
  |---|---|
  | `konfetti.base` | The Kreate plugin, the group and the version |
  | `konfetti.dependency-locking` | Strict locking of every configuration |
  | `konfetti.git-hooks` | The Git hooks |
  | `konfetti.trivy` | Every Trivy scan |
  | `konfetti.assurance` | The assurance gate and its `ignore` rules |
  | `konfetti.standards` | The standards konfetti follows, reviewed in `standards.lock` |
  | `konfetti.platform` | The JDK, the Kotlin compiler, the targets and the Java module name |
  | `konfetti.web-tools` | Node.js and Yarn for the Wasm target |
  | `konfetti.testing` | Kotest and the test suites |
  | `konfetti.detekt` | Detekt and its reports |
  | `konfetti.coverage` | Kover and its verification |
  | `konfetti.api-validation` | The API dump and the semver check |
  | `konfetti.docs` | Dokka |
  | `konfetti.publish` | The POM, the release guard and the Davils repository |
  | `konfetti.archives` | The legal files and the manifest of every archive |
  | `konfetti.tool-classpaths` | The version floors on the class paths of the build tools |
  | `konfetti.changelog` | The changelog check |
  | `konfetti.license-header` | The license header check |

- Dependencies are declared in `gradle/libs.versions.toml`. Libraries of JetBrains and of Kore may be added when they
  are needed; every other dependency needs the approval of a maintainer.
- Only `repo.davils.com` serves `com.davils`, through `exclusiveContent`; nothing is resolved from `mavenLocal()`.
- After a dependency changes, refresh the locks and the verification metadata in one run and review what changed:

  ```bash
  ./gradlew kreateResolveAndLockAll :build-logic:dependencies --write-locks --write-verification-metadata pgp,sha256 --export-keys --no-configuration-cache
  ```

- konfetti builds only with released versions of Kreate and Kore, trusted by the Davils release key
  `36F578E8AD2D4391E0FA6C18154C625FAD370063`; no trust rule for unsigned snapshots is ever added.
- An `ignore` rule of the assurance gate has a reason, the exact module and an expiry no more than six months ahead.
  CRITICAL and HIGH findings can never be ignored.
- Every standard konfetti follows is declared in `build-logic/src/main/kotlin/konfetti.standards.gradle.kts` and
  reviewed with `./gradlew kreateStandardsReview --standard=<id> --note="..."`.

## Kore internals

konfetti opts into these `@DavilsInternal` declarations of Kore; Kore changes them only in a major version:
`SafeText.text` and `SafeText.REDACTED`, `OrderedMap`, and since 1.1.0 `guard`, `grownDelay` and `fullJitter`.
A new use is added here and to the table of Kore's
`AGENTS.md`.

## Public API

The `api/*.api` dumps are the contract. `kreateApiCheck` fails on every unrecorded change and `kreateApiSemverCheck`
on a version that does not match it. A deliberate change is recorded with `./gradlew kreateApiDump` and gets a
`CHANGELOG.md` entry. A breaking change needs a major version and a commit marked as breaking. Deprecate before
removing: `@Deprecated` with a `ReplaceWith`, for at least one minor release. `envelopeOf` is `@DavilsInternal` and
exists for `konfetti-testing` only.

## Releases

The version lives in `gradle.properties` and nowhere else; `kreateReleaseGuard` refuses every other version. Every
build without a release tag is a `-SNAPSHOT`. A release is a tag without a `v`, such as `1.0.0`, on a commit of `main`.
Snapshots go to `https://repo.davils.com/snapshots`, releases to `https://repo.davils.com/releases`, never to Maven
Central. The pipeline publishes with `publishAllPublicationsToDavilsRepository --no-configuration-cache` and signs
with `ORG_GRADLE_PROJECT_signingInMemoryKey`, `…KeyId` and `…KeyPassword`.

## Git

Work on `develop`, release from `main`. Conventional Commits in English with the module as scope, such as
`feat(core): ...` or `fix(testing): ...`. Small commits, one concern each. Commit only when asked. No
`Co-Authored-By` trailer and no other attribution to tools or AI assistants.
