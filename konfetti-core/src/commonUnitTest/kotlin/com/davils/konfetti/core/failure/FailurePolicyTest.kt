/*
 * Copyright 2026 Davils
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.davils.konfetti.core.failure

import com.davils.konfetti.core.consume.ConsumerState
import com.davils.konfetti.core.consume.Consumption
import com.davils.konfetti.core.currentAttempt
import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.konfetti.core.fixture.AUDITOR
import com.davils.konfetti.core.fixture.Broken
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.UNINDEXABLE
import com.davils.konfetti.core.fixture.attach
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.core.fixture.send
import com.davils.konfetti.core.fixture.settle
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.testing.EventRecorder
import com.davils.konfetti.testing.RecordingObserver
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.testScheduler
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.comparables.shouldBeLessThanOrEqualTo
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private const val THREE: Int = 3
private const val FOUR: Int = 4
private const val HUNDRED: Int = 100
private const val TWO_HUNDRED: Int = 200
private const val CAP: Int = 250
private const val HALF: Double = 0.5

class FailurePolicyTest : FunSpec({
    context("skipping").config(coroutineTestScope = true) {
        test("reports the failure and carries on with the next envelope") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val recorder = EventRecorder<DeviceEvent>()
            val consumer = bus[Topics.devices].attach(INDEXER) {
                onFailure = FailurePolicy.Skip
                handle { envelope ->
                    if (envelope.payload.id == "bad") throw Broken(UNINDEXABLE)
                    recorder.record(envelope)
                }
            }

            bus[Topics.devices].send(DeviceEvent.Registered("bad"))
            bus[Topics.devices].send(DeviceEvent.Registered("good"))

            val handled = recorder.awaitCount(1).single()
            handled.payload.id shouldBe "good"
            val failed = observer.only<BusObservation.Failed>().single()
            failed.cause.message shouldBe UNINDEXABLE
            observer.only<BusObservation.Skipped>().single().attempts shouldBe 1
            consumer.metrics.failed shouldBe 1
            consumer.metrics.skipped shouldBe 1
        }

        test("treats a cancellation thrown by a handler that is still running as a failure") {
            val bus = devices()
            val consumer = bus[Topics.devices].attach(INDEXER) {
                onFailure = FailurePolicy.Skip
                handle { throw CancellationException("not mine") }
            }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            settle()

            consumer.metrics.skipped shouldBe 1
            consumer.state.value shouldBe ConsumerState.Running
        }
    }

    context("stopping").config(coroutineTestScope = true) {
        test("stops the consumer at its first failure and drops what it still holds") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val consumer = bus[Topics.devices].attach(INDEXER) {
                onFailure = FailurePolicy.Stop
                handle { throw Broken("fatal") }
            }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            bus[Topics.devices].send(DeviceEvent.Registered("r2"))
            settle()

            val stopped = consumer.state.value.shouldBeInstanceOf<ConsumerState.Stopped>()
            stopped.cause.message shouldBe "fatal"
            observer.only<BusObservation.ConsumerStopped>().single().cause shouldBe stopped.cause
            val reasons = observer.only<BusObservation.Dropped>().map { dropped -> dropped.reason }
            reasons.distinct() shouldBe listOf(DropReason.Stopped)
            consumer.metrics.dropped shouldBe 2
            bus[Topics.devices].metrics.consumers shouldBe 0
        }
    }

    context("dead letters").config(coroutineTestScope = true) {
        test("sends what failed to the dead letters by default") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val letters = EventRecorder<DeadLetter>()
            bus.deadLetters.attach("ops.dead-letters") {
                onFailure = FailurePolicy.Skip
                handle(letters::record)
            }
            val consumer = bus[Topics.devices].attach(INDEXER) { handle { throw Broken(UNINDEXABLE) } }

            val original = bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            val letter = letters.awaitCount(1).single()
            letter.payload.original shouldBe original
            letter.payload.consumer shouldBe consumer.name
            letter.payload.attempts shouldBe 1
            letter.payload.cause.message shouldBe UNINDEXABLE
            letter.causationId shouldBe original.id
            letter.payload.toString() shouldContain "consumer=inventory.indexer"
            observer.only<BusObservation.DeadLettered>().single().consumer shouldBe consumer.name
            consumer.metrics.deadLettered shouldBe 1
        }

        test("drops what failed when the dead letters no longer take anything") {
            val bus = devices()
            bus.deadLetters.runtime.abort(DropReason.Closed)
            val consumer = bus[Topics.devices].attach(INDEXER) { handle { throw Broken(UNINDEXABLE) } }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            settle()

            consumer.metrics.dropped shouldBe 1
            consumer.metrics.deadLettered shouldBe 0
        }

        test("refuses to send dead letters back to the dead letters") {
            val consumption = devices().deadLetters.consume("ops.dead-letters") {
                onFailure = retry(attempts = 2, backoff = fixed(1.seconds))
                handle { }
            }

            val invalid = consumption.shouldBeInstanceOf<Consumption.Invalid>()
            invalid.report.errors
                .single()
                .code shouldBe EventViolationCodes.DEAD_LETTER_LOOP
        }
    }

    context("retrying").config(coroutineTestScope = true) {
        test("tries again after a fixed delay until the handler succeeds") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val attempts = mutableListOf<Int?>()
            val done = CompletableDeferred<Duration>()
            val start = testScheduler.timeSource.markNow()
            bus[Topics.devices].attach(INDEXER) {
                onFailure = retry(attempts = THREE, backoff = fixed(1.seconds))
                handle {
                    attempts += currentAttempt()
                    if (attempts.size < THREE) throw Broken("not yet")
                    done.complete(start.elapsedNow())
                }
            }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            done.await() shouldBe 2.seconds
            attempts shouldContainExactly listOf(1, 2, THREE)
            observer.only<BusObservation.Retrying>().map { retrying -> retrying.attempt } shouldContainExactly
                listOf(2, THREE)
            observer.only<BusObservation.Delivered>().single().attempt shouldBe THREE
        }

        test("grows the delay exponentially up to the maximum and then gives up") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val consumer = bus[Topics.devices].attach(INDEXER) {
                onFailure = retry(
                    attempts = FOUR,
                    backoff = exponential(initial = HUNDRED.milliseconds, max = CAP.milliseconds),
                    then = FailurePolicy.Skip,
                )
                handle { throw Broken("never") }
            }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            settle()

            observer.only<BusObservation.Retrying>().map { retrying -> retrying.delay } shouldContainExactly
                listOf(HUNDRED.milliseconds, TWO_HUNDRED.milliseconds, CAP.milliseconds)
            observer.only<BusObservation.Skipped>().single().attempts shouldBe FOUR
            consumer.metrics.retried shouldBe THREE
            consumer.metrics.failed shouldBe FOUR
        }

        test("spreads the delay below its ceiling when it jitters") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            bus[Topics.devices].attach(INDEXER) {
                onFailure = retry(
                    attempts = 2,
                    backoff = exponential(initial = 1.seconds, max = 1.seconds, jitter = true),
                    then = FailurePolicy.Skip,
                )
                handle { throw Broken("never") }
            }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            settle()

            observer.only<BusObservation.Retrying>().single().delay shouldBeLessThanOrEqualTo 1.seconds
        }
    }

    context("defaults") {
        test("retry with a growing delay and end in the dead letters") {
            val policy = retry(attempts = 2)

            policy.then shouldBe FailurePolicy.DeadLetter
            policy.backoff shouldBe exponential(initial = HUNDRED.milliseconds, max = 10.seconds)
        }
    }

    context("timeouts").config(coroutineTestScope = true) {
        test("fails a handling that takes longer than allowed") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            bus[Topics.devices].attach(INDEXER) {
                handlingTimeout = 1.seconds
                onFailure = FailurePolicy.Skip
                handle { delay(1.seconds * 2) }
            }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            settle()

            val failure = observer.only<BusObservation.Failed>().single().cause
            val timeout = failure.shouldBeInstanceOf<HandlingTimeoutException>()
            timeout.timeout shouldBe 1.seconds
            timeout.message shouldContain "within 1s"
        }
    }

    context("validation").config(coroutineTestScope = true) {
        test("reports a retry and a backoff that cannot work, under the policy") {
            val topic = devices()[Topics.devices]

            val retries = topic.consume(INDEXER) {
                onFailure = retry(attempts = 0, backoff = fixed((-1).seconds))
                handle { }
            }

            val invalid = retries.shouldBeInstanceOf<Consumption.Invalid>()
            invalid.report.errors.map { violation -> violation.path.toString() to violation.code } shouldContainExactly
                listOf(
                    "onFailure.attempts" to EventViolationCodes.INVALID_RETRY,
                    "onFailure.backoff.delay" to EventViolationCodes.INVALID_BACKOFF,
                )
        }

        test("refuses an exponential backoff that does not grow sensibly") {
            val topic = devices()[Topics.devices]

            val consumption = topic.consume(INDEXER) {
                onFailure = retry(
                    attempts = 2,
                    backoff = exponential(initial = Duration.ZERO, max = Duration.INFINITE, factor = HALF),
                )
                handle { }
            }

            val invalid = consumption.shouldBeInstanceOf<Consumption.Invalid>()
            invalid.report.errors.map { violation -> violation.path.toString() } shouldContainExactly listOf(
                "onFailure.backoff.initial",
                "onFailure.backoff.max",
                "onFailure.backoff.factor",
            )
        }

        test("refuses a maximum below the initial delay and an endless fixed delay") {
            val topic = devices()[Topics.devices]

            val shrinking = topic.consume(INDEXER) {
                onFailure = retry(attempts = 2, backoff = exponential(initial = 1.seconds, max = HUNDRED.milliseconds))
                handle { }
            }
            val endless = topic.consume(AUDITOR) {
                onFailure = retry(attempts = 2, backoff = fixed(Duration.INFINITE))
                handle { }
            }

            val tooShort = shrinking.shouldBeInstanceOf<Consumption.Invalid>().report
            val tooLong = endless.shouldBeInstanceOf<Consumption.Invalid>().report
            tooShort.errors.map { violation -> violation.path.toString() } shouldBe listOf("onFailure.backoff.max")
            tooLong.errors.map { violation -> violation.path.toString() } shouldBe listOf("onFailure.backoff.delay")
        }
    }
})
