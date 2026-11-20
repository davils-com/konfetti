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
import com.davils.konfetti.core.currentAttempt
import com.davils.konfetti.core.currentEnvelope
import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.konfetti.core.fixture.AUDITOR
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.attach
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.core.fixture.send
import com.davils.konfetti.core.fixture.settle
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.testing.EventRecorder
import com.davils.konfetti.testing.RecordingObserver
import com.davils.kore.core.validation.violation.ViolationCode
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.backgroundScope
import io.kotest.engine.coroutines.testScheduler
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

private const val PARTITIONS: Int = 4
private const val KEYS: Int = 3
private const val EVENTS_PER_KEY: Int = 5
private const val STEP: Int = 7
private const val TOO_MANY: Int = 5

private val unscoped = object : CoroutineScope {
    override val coroutineContext: CoroutineContext = EmptyCoroutineContext
}

class ConsumeTest : FunSpec({
    context("delivery").config(coroutineTestScope = true) {
        test("hands every envelope to every consumer") {
            val bus = devices()
            val indexer = EventRecorder<DeviceEvent>()
            val auditor = EventRecorder<DeviceEvent>()
            bus[Topics.devices].attach(INDEXER) { handle(indexer::record) }
            bus[Topics.devices].attach(AUDITOR) { handle(auditor::record) }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            bus[Topics.devices].send(DeviceEvent.Removed("r1"))

            indexer.awaitCount(2)
            auditor.awaitCount(2)
            indexer.payloads shouldContainExactly listOf(DeviceEvent.Registered("r1"), DeviceEvent.Removed("r1"))
            auditor.payloads shouldBe indexer.payloads
        }

        test("keeps the order per key while workers run side by side") {
            val bus = bus {
                topic(Topics.devices) {
                    partitions = PARTITIONS
                    orderBy { event -> event.id }
                }
            }
            val recorder = EventRecorder<DeviceEvent>()
            bus[Topics.devices].attach(INDEXER) {
                concurrency = PARTITIONS
                handle { envelope ->
                    delay(
                        (
                            envelope.payload.id
                                .hashCode()
                                .mod(STEP) +
                                1
                            ).milliseconds,
                    )
                    recorder.record(envelope)
                }
            }

            repeat(EVENTS_PER_KEY) { round ->
                repeat(
                    KEYS,
                ) { key -> bus[Topics.devices].send(DeviceEvent.Renamed(id = "r$key", name = "name-$round")) }
            }

            recorder.awaitCount(KEYS * EVENTS_PER_KEY)
            val names = recorder.payloads.filterIsInstance<DeviceEvent.Renamed>().groupBy { event -> event.id }
            names.values.forEach { renames ->
                renames.map { rename -> rename.name } shouldContainExactly List(EVENTS_PER_KEY) { "name-$it" }
            }
        }

        test("reports what it handled") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val recorder = EventRecorder<DeviceEvent>()
            val consumer = bus[Topics.devices].attach(INDEXER) { handle(recorder::record) }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            recorder.awaitCount(1)
            testScheduler.runCurrent()

            consumer.metrics.received shouldBe 1
            consumer.metrics.delivered shouldBe 1
            consumer.metrics.queued shouldBe 0
            consumer.metrics.settled shouldBe 1
            observer.only<BusObservation.ConsumerAttached>().single().consumer shouldBe consumer.name
            observer.only<BusObservation.Delivered>().single().attempt shouldBe 1
        }

        test("tells the handler which envelope and attempt it is handling") {
            val bus = devices()
            val seen = CompletableDeferred<Pair<Envelope<Event>?, Int?>>()
            bus[Topics.devices].attach(INDEXER) {
                handle { seen.complete(currentEnvelope() to currentAttempt()) }
            }

            val envelope = bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            seen.await() shouldBe (envelope to 1)
            currentEnvelope() shouldBe null
            currentAttempt() shouldBe null
        }
    }

    context("start position").config(coroutineTestScope = true) {
        test("hands a retained consumer the latest envelopes before anything new") {
            val bus = bus { topic(Topics.devices) { retain = 2 } }
            val topic = bus[Topics.devices]
            topic.send(DeviceEvent.Registered("r1"))
            topic.send(DeviceEvent.Registered("r2"))
            topic.send(DeviceEvent.Registered("r3"))
            testScheduler.runCurrent()
            val recorder = EventRecorder<DeviceEvent>()
            topic.attach(INDEXER) {
                start = StartPosition.Retained
                handle(recorder::record)
            }

            topic.send(DeviceEvent.Registered("r4"))

            recorder.awaitCount(KEYS)
            recorder.payloads.map { event -> event.id } shouldContainExactly listOf("r2", "r3", "r4")
            topic.metrics.retained shouldBe 2
        }

        test("hands a consumer that starts at the latest only what comes after it") {
            val bus = bus { topic(Topics.devices) { retain = 2 } }
            val topic = bus[Topics.devices]
            topic.send(DeviceEvent.Registered("r1"))
            testScheduler.runCurrent()
            val recorder = EventRecorder<DeviceEvent>()
            topic.attach(INDEXER) { handle(recorder::record) }

            topic.send(DeviceEvent.Registered("r2"))

            recorder.awaitCount(1).map { envelope -> envelope.payload.id } shouldContainExactly listOf("r2")
        }
    }

    context("selection").config(coroutineTestScope = true) {
        test("narrows the payload to one variant of a sealed event") {
            val bus = devices()
            val renames = EventRecorder<DeviceEvent.Renamed>()
            val consumer = bus[Topics.devices].attach("inventory.renames") {
                handleOnly<DeviceEvent.Renamed>(renames::record)
            }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            bus[Topics.devices].send(DeviceEvent.Renamed(id = "r1", name = "core"))

            val rename = renames.awaitCount(1).single()
            rename.payload.name shouldBe "core"
            consumer.metrics.filtered shouldBe 1
        }

        test("hands on only what every predicate accepts") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val recorder = EventRecorder<DeviceEvent>()
            bus[Topics.devices].attach("inventory.core") {
                where { envelope -> envelope.payload.id.startsWith("core") }
                where { envelope -> envelope.payload is DeviceEvent.Registered }
                handle(recorder::record)
            }

            bus[Topics.devices].send(DeviceEvent.Registered("edge-1"))
            bus[Topics.devices].send(DeviceEvent.Removed("core-1"))
            bus[Topics.devices].send(DeviceEvent.Registered("core-2"))

            val core = recorder.awaitCount(1).single()
            core.payload.id shouldBe "core-2"
            observer.count<BusObservation.Filtered>() shouldBe 2
        }
    }

    context("interception").config(coroutineTestScope = true) {
        test("wraps each handling, outermost first") {
            val calls = mutableListOf<String>()
            val bus = devices {
                intercept { _, consumer, proceed ->
                    calls += "outer $consumer"
                    proceed()
                }
                intercept { envelope, _, proceed ->
                    calls += "inner ${envelope.topic}"
                    proceed()
                }
            }
            val done = CompletableDeferred<Unit>()
            bus[Topics.devices].attach(INDEXER) {
                handle {
                    calls += "handler"
                    done.complete(Unit)
                }
            }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            done.await()

            calls shouldContainExactly listOf("outer inventory.indexer", "inner inventory.devices", "handler")
        }

        test("keeps an envelope from a consumer whose interceptor does not proceed") {
            val bus = devices { intercept { _, _, _ -> } }
            val consumer = bus[Topics.devices].attach(INDEXER) { handle { } }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            settle()

            consumer.metrics.filtered shouldBe 1
            consumer.metrics.delivered shouldBe 0
        }
    }

    context("attaching").config(coroutineTestScope = true) {
        test("refuses a second consumer of the same name") {
            val topic = devices()[Topics.devices]
            topic.attach(INDEXER) { handle { } }

            val second = topic.consume(INDEXER) { handle { } }

            second shouldBe Consumption.Duplicate(Topics.devices.name, ConsumerName(INDEXER))
            shouldThrow<IllegalStateException> { second.getOrThrow() }
        }

        test("reports every problem of a consumer at once") {
            val topic = devices()[Topics.devices]

            val consumption = topic.consume("Indexer") {
                concurrency = 0
                mailboxCapacity = 0
                handlingTimeout = Duration.ZERO
            }

            val invalid = consumption.shouldBeInstanceOf<Consumption.Invalid>()
            invalid.report.errors.map { violation -> violation.code } shouldContainExactly listOf(
                EventViolationCodes.INVALID_NAME,
                EventViolationCodes.INVALID_CONCURRENCY,
                EventViolationCodes.INVALID_MAILBOX,
                EventViolationCodes.INVALID_TIMEOUT,
                EventViolationCodes.MISSING_HANDLER,
            )
            shouldThrow<IllegalArgumentException> { consumption.getOrThrow() }
        }

        test("refuses more workers than partitions and more than one handler") {
            val topic = devices()[Topics.devices]

            val consumption = topic.consume(INDEXER) {
                concurrency = TOO_MANY
                handle { }
                handle { }
            }

            val invalid = consumption.shouldBeInstanceOf<Consumption.Invalid>()
            invalid.report.errors.map { violation -> violation.code } shouldContainExactly listOf(
                EventViolationCodes.INVALID_CONCURRENCY,
                ViolationCode.DUPLICATE,
            )
        }

        test("refuses a consumer once the topic is closed") {
            val bus = devices()
            bus.close()

            val consumption = bus[Topics.devices].consume(INDEXER) { handle { } }

            consumption shouldBe Consumption.Closed(Topics.devices.name)
            shouldThrow<IllegalStateException> { consumption.getOrThrow() }
        }

        test("says whether anybody would receive a publication") {
            val bus = devices()
            val topic = bus[Topics.devices]
            val retaining = bus.declare(Topics.audits) { retain = 1 }

            topic.hasConsumers shouldBe false
            retaining.getOrThrow().hasConsumers shouldBe true
            val consumer = topic.attach(INDEXER) { handle { } }
            topic.hasConsumers shouldBe true
            consumer.detach()
            topic.hasConsumers shouldBe false
        }
    }

    context("detaching").config(coroutineTestScope = true) {
        test("stops a consumer from receiving and reports it once") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val recorder = EventRecorder<DeviceEvent>()
            val consumer = bus[Topics.devices].attach(INDEXER) { handle(recorder::record) }

            consumer.detach()
            consumer.detach()
            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            settle()

            consumer.state.value shouldBe ConsumerState.Detached
            recorder.envelopes shouldBe emptyList()
            observer.count<BusObservation.ConsumerDetached>() shouldBe 1
            consumer.toString() shouldBe "Consumer(inventory.devices/inventory.indexer)"
            consumer.topic shouldBe Topics.devices.name
        }

        test("detaches a consumer when its owner ends") {
            val bus = devices()
            val owner = Job()
            val consumer = bus[Topics.devices].consume(INDEXER, CoroutineScope(owner)) { handle { } }

            owner.cancel()

            consumer.getOrThrow().state.value shouldBe ConsumerState.Detached
        }

        test("keeps a consumer whose owner has no job") {
            val bus = devices()
            val consumer = bus[Topics.devices].consume(INDEXER, unscoped) { handle { } }

            consumer.getOrThrow().state.value shouldBe ConsumerState.Running
        }
    }

    context("flows").config(coroutineTestScope = true) {
        test("streams what is published while it is collected") {
            val bus = devices()
            val topic = bus[Topics.devices]
            val events = topic.events()
            val collected = backgroundScope.async { events.take(2).toList() }
            testScheduler.runCurrent()

            topic.send(DeviceEvent.Registered("r1"))
            topic.send(DeviceEvent.Removed("r1"))

            collected.await().map { envelope -> envelope.payload } shouldContainExactly
                listOf(DeviceEvent.Registered("r1"), DeviceEvent.Removed("r1"))
            testScheduler.runCurrent()
            topic.metrics.consumers shouldBe 0
        }

        test("ends at once on a closed bus") {
            val bus = devices()
            bus.close()

            bus[Topics.devices].events().toList() shouldBe emptyList()
        }
    }
})
