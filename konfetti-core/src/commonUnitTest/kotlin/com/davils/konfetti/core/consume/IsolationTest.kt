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

package com.davils.konfetti.core.consume

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.bus.Entropy
import com.davils.konfetti.core.failure.FailurePolicy
import com.davils.konfetti.core.failure.exponential
import com.davils.konfetti.core.failure.retry
import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.attach
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.core.fixture.send
import com.davils.konfetti.core.fixture.settle
import com.davils.konfetti.core.lifecycle.BusState
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.BusObserver
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.testing.RecordingObserver
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlin.time.Duration.Companion.seconds

private const val STALLED_PUBLICATIONS: Int = 4
private const val STALLED_DROPS: Long = 2
private const val FAILING_PUBLICATIONS: Int = 80
private val stallTime = 5.seconds

private val throwingObserver = object : BusObserver {
    override val isObserving: Boolean get() = error("broken")

    override fun observe(observation: BusObservation) {
        error("never called")
    }
}

class IsolationTest : FunSpec({
    context("consume interceptors").config(coroutineTestScope = true) {
        test("fail the attempt instead of running the handler twice when they proceed twice") {
            val observer = RecordingObserver()
            var handled = 0
            val bus = devices {
                observe(observer)
                intercept { _, _, proceed ->
                    proceed()
                    proceed()
                }
            }
            val consumer = bus[Topics.devices].attach(INDEXER) {
                onFailure = FailurePolicy.Skip
                handle { handled += 1 }
            }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            settle()

            handled shouldBe 1
            consumer.metrics.skipped shouldBe 1
            val failed = observer.only<BusObservation.Failed>().single()
            failed.cause.message shouldContain "more than once"
        }
    }

    context("observers").config(coroutineTestScope = true) {
        test("whose isObserving throws count as not observing and never disturb the bus") {
            val bus = devices { observe(throwingObserver) }
            val consumer = bus[Topics.devices].attach(INDEXER) { handle { } }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            settle()

            consumer.metrics.delivered shouldBe 1
            bus.snapshot().observerFailures shouldBe 0
        }
    }

    context("entropy").config(coroutineTestScope = true) {
        test("that throws gives no jitter and still lets the bus pick partitions") {
            val bus = devices { entropy = Entropy { error("no randomness") } }
            val consumer = bus[Topics.devices].attach(INDEXER) {
                onFailure = retry(attempts = 2, backoff = exponential(1.seconds, 1.seconds, jitter = true))
                handle { error("fails") }
            }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            settle()

            consumer.metrics.retried shouldBe 1
            consumer.metrics.deadLettered shouldBe 1
        }
    }

    context("the longest stall of a consumer").config(coroutineTestScope = true) {
        test("drops an envelope for a stuck consumer while the other consumers of the partition go on") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val topic = bus[Topics.devices]
            val stuck = topic.attach("inventory.stuck") {
                mailboxCapacity = 1
                maximumStall = stallTime
                handle { awaitCancellation() }
            }
            val healthy = mutableListOf<Envelope<Event>>()
            topic.attach(INDEXER) { handle { envelope -> healthy += envelope } }

            repeat(STALLED_PUBLICATIONS) { index -> topic.send(DeviceEvent.Registered("r$index")) }
            settle()

            val reasons = observer.only<BusObservation.Dropped>().map { dropped -> dropped.reason }
            healthy.size shouldBe STALLED_PUBLICATIONS
            stuck.metrics.dropped shouldBe STALLED_DROPS
            reasons.toSet() shouldBe setOf(DropReason.Stalled)
        }
    }

    context("the dead-letter stall").config(coroutineTestScope = true) {
        test("drops what cannot be dead-lettered in time instead of blocking the failing consumer") {
            val bus = devices { deadLetterStall = 1.seconds }
            bus.deadLetters.attach("ops.dead-letters") {
                mailboxCapacity = 1
                onFailure = FailurePolicy.Skip
                handle { awaitCancellation() }
            }
            val failing = bus[Topics.devices].attach(INDEXER) { handle { error("fails") } }

            repeat(FAILING_PUBLICATIONS) { index -> bus[Topics.devices].send(DeviceEvent.Registered("r$index")) }
            settle()

            val metrics = failing.metrics
            metrics.received shouldBe FAILING_PUBLICATIONS.toLong()
            metrics.settled shouldBe FAILING_PUBLICATIONS.toLong()
            (metrics.dropped > 0) shouldBe true
            metrics.deadLettered + metrics.dropped shouldBe FAILING_PUBLICATIONS.toLong()
        }
    }

    context("publishing without waiting from a handler").config(coroutineTestScope = true) {
        test("takes the handled envelope as its cause") {
            val bus = devices { topic(Topics.audits) }
            val caused = CompletableDeferred<Envelope<Event>>()
            bus[Topics.audits].attach("audit.reader") { handle { envelope -> caused.complete(envelope) } }
            bus[Topics.devices].attach(INDEXER) {
                handle { bus[Topics.audits].tryPublish(AuditEvent("seen")) }
            }

            val origin = bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            settle()

            caused.await().causationId shouldBe origin.id
            caused.await().correlationId shouldBe origin.id
        }
    }

    context("aborting a bus").config(coroutineTestScope = true) {
        test("closes it without waiting, once") {
            val bus = devices()

            bus.abort() shouldBe true
            bus.abort() shouldBe false
            settle()

            bus.state.value shouldBe BusState.Closed
        }
    }
})
