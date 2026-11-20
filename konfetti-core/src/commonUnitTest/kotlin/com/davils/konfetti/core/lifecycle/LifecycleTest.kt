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

package com.davils.konfetti.core.lifecycle

import com.davils.konfetti.core.consume.ConsumerState
import com.davils.konfetti.core.failure.FailurePolicy
import com.davils.konfetti.core.failure.fixed
import com.davils.konfetti.core.failure.retry
import com.davils.konfetti.core.fixture.AUDITOR
import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.fixture.Broken
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.attach
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.core.fixture.send
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.testing.EventRecorder
import com.davils.konfetti.testing.RecordingObserver
import com.davils.konfetti.testing.testEventBus
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
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

private const val THREE: Int = 3

class LifecycleTest : FunSpec({
    context("draining").config(coroutineTestScope = true) {
        test("delivers everything it holds and then closes") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val recorder = EventRecorder<DeviceEvent>()
            bus[Topics.devices].attach(INDEXER) {
                handle { envelope ->
                    delay(1.seconds)
                    recorder.record(envelope)
                }
            }
            listOf("r1", "r2", "r3").forEach { id -> bus[Topics.devices].send(DeviceEvent.Registered(id)) }

            val result = bus.drain(1.hours)

            result shouldBe DrainResult.Completed
            recorder.envelopes.size shouldBe THREE
            bus.state.value shouldBe BusState.Closed
            bus[Topics.devices].state shouldBe BusState.Closed
            observer.only<BusObservation.StateChanged>().map { changed -> changed.state } shouldContainExactly
                listOf(BusState.Draining, BusState.Closed)
        }

        test("waits for retries that are still due") {
            val bus = devices()
            val attempts = mutableListOf<Duration>()
            val start = testScheduler.timeSource.markNow()
            bus[Topics.devices].attach(INDEXER) {
                onFailure = retry(attempts = THREE, backoff = fixed(1.seconds), then = FailurePolicy.Skip)
                handle {
                    attempts += start.elapsedNow()
                    throw Broken("never")
                }
            }
            bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            bus.drain(1.hours) shouldBe DrainResult.Completed

            attempts shouldContainExactly listOf(Duration.ZERO, 1.seconds, 2.seconds)
        }

        test("still takes what a consumer publishes onto a topic declared after its own") {
            val bus = bus {
                topic(Topics.devices)
                topic(Topics.audits)
            }
            val recorder = EventRecorder<AuditEvent>()
            bus[Topics.audits].attach("audit.writer") { handle(recorder::record) }
            bus[Topics.devices].attach(AUDITOR) {
                handle { envelope -> bus[Topics.audits].publish(AuditEvent("saw ${envelope.payload.id}")) }
            }
            bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            bus.drain(1.hours)

            recorder.payloads shouldContainExactly listOf(AuditEvent("saw r1"))
        }

        test("abandons what is left when it runs out of time") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val consumer = bus[Topics.devices].attach(INDEXER) { handle { awaitCancellation() } }
            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            bus[Topics.devices].send(DeviceEvent.Registered("r2"))

            val result = bus.drain(1.seconds)

            result shouldBe DrainResult.TimedOut(abandoned = 2)
            consumer.state.value shouldBe ConsumerState.Detached
            val reasons = observer.only<BusObservation.Dropped>().map { dropped -> dropped.reason }
            reasons.distinct() shouldBe listOf(DropReason.Abandoned)
        }

        test("answers a second drain with the outcome of the first") {
            val bus = devices()

            bus.drain(1.seconds) shouldBe DrainResult.Completed
            bus.drain(1.seconds) shouldBe DrainResult.Completed
            bus.close() shouldBe CloseReport(dropped = 0)
        }
    }

    context("closing").config(coroutineTestScope = true) {
        test("drops what is in flight and reports how much") {
            val bus = devices()
            val started = CompletableDeferred<Unit>()
            bus[Topics.devices].attach(INDEXER) {
                handle {
                    started.complete(Unit)
                    awaitCancellation()
                }
            }
            listOf("r1", "r2", "r3").forEach { id -> bus[Topics.devices].send(DeviceEvent.Registered(id)) }
            started.await()

            bus.close() shouldBe CloseReport(dropped = THREE.toLong())
            bus.close() shouldBe CloseReport(dropped = 0)
            bus.drain(1.seconds) shouldBe DrainResult.AlreadyClosed
        }

        test("drops what is still waiting on a partition") {
            val bus = devices()
            bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            bus.close() shouldBe CloseReport(dropped = 1)
            bus[Topics.devices].metrics.dropped shouldBe 1
        }

        test("closes when the scope it runs in ends") {
            val job = Job()
            val bus = testEventBus(CoroutineScope(job + StandardTestDispatcher(testScheduler)))

            job.cancel()
            testScheduler.runCurrent()

            bus.state.value shouldBe BusState.Closed
            bus.drain(1.seconds) shouldBe DrainResult.AlreadyClosed
        }

        test("stays closed when the scope it runs in ends after it was closed") {
            val job = Job()
            val bus = testEventBus(CoroutineScope(job + StandardTestDispatcher(testScheduler)))
            bus.close()

            job.cancel()
            testScheduler.runCurrent()

            bus.drain(1.seconds) shouldBe DrainResult.AlreadyClosed
        }

        test("drops what a consumer handles when the scope it runs in ends") {
            val job = Job()
            val observer = RecordingObserver()
            val bus = testEventBus(CoroutineScope(job + StandardTestDispatcher(testScheduler))) {
                observe(observer)
                topic(Topics.devices)
            }
            bus[Topics.devices].attach(INDEXER) { handle { awaitCancellation() } }
            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            testScheduler.runCurrent()

            job.cancel()
            testScheduler.runCurrent()

            observer.only<BusObservation.Dropped>().single().reason shouldBe DropReason.Closed
        }

        test("gives way to a close that arrives while it drains") {
            val bus = devices()
            bus[Topics.devices].attach(INDEXER) { handle { awaitCancellation() } }
            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            val draining = backgroundScope.async { bus.drain(1.hours) }
            testScheduler.runCurrent()

            bus.close()

            draining.await() shouldBe DrainResult.AlreadyClosed
        }

        test("drains a bus whose scope has no job") {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val scope = object : CoroutineScope {
                override val coroutineContext: CoroutineContext = dispatcher
            }

            testEventBus(scope).drain(1.seconds) shouldBe DrainResult.Completed
        }

        test("runs in a scope without a job until it is closed") {
            val dispatcher = StandardTestDispatcher(testScheduler)
            val scope = object : CoroutineScope {
                override val coroutineContext: CoroutineContext = dispatcher
            }
            val bus = testEventBus(scope)

            bus.state.value shouldBe BusState.Running
            bus.close()
            bus.state.value shouldBe BusState.Closed
        }
    }

    context("draining a consumer").config(coroutineTestScope = true) {
        test("finishes what it holds and detaches") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val recorder = EventRecorder<DeviceEvent>()
            val consumer = bus[Topics.devices].attach(INDEXER) {
                handle { envelope ->
                    delay(1.seconds)
                    recorder.record(envelope)
                }
            }
            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            testScheduler.runCurrent()

            consumer.drain(1.hours) shouldBe DrainResult.Completed
            consumer.drain(1.hours) shouldBe DrainResult.AlreadyClosed

            recorder.envelopes.size shouldBe 1
            consumer.state.value shouldBe ConsumerState.Detached
            observer.count<BusObservation.ConsumerDetached>() shouldBe 1
        }

        test("abandons what it holds when it runs out of time") {
            val bus = devices()
            val consumer = bus[Topics.devices].attach(INDEXER) { handle { awaitCancellation() } }
            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            testScheduler.runCurrent()

            val result = consumer.drain(1.seconds)

            result.shouldBeInstanceOf<DrainResult.TimedOut>().abandoned shouldBe 1
        }
    }
})
