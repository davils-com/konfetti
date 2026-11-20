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
import com.davils.konfetti.core.failure.DeadLetter
import com.davils.konfetti.core.fixture.AUDITOR
import com.davils.konfetti.core.fixture.Broken
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.UNINDEXABLE
import com.davils.konfetti.core.fixture.attach
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.core.fixture.send
import com.davils.konfetti.core.internal.bus.DeadLetterOutcome
import com.davils.konfetti.core.internal.bus.DeadLetterSink
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.testing.EventRecorder
import com.davils.konfetti.testing.RecordingObserver
import com.davils.konfetti.testing.testEnvelope
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.testScheduler
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation

private const val THREE: Int = 3

class ConsumerShutdownTest : FunSpec({
    context("a consumer that is held up").config(coroutineTestScope = true) {
        test("drops what a blocked publisher still holds for it when it detaches") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }
            val blocked = bus[Topics.devices].attach(INDEXER) {
                mailboxCapacity = 1
                handle { awaitCancellation() }
            }
            listOf("r1", "r2", "r3").forEach { id ->
                bus[Topics.devices].send(DeviceEvent.Registered(id))
                testScheduler.runCurrent()
            }

            blocked.detach()
            testScheduler.runCurrent()

            val reasons = observer.only<BusObservation.Dropped>().map { dropped -> dropped.reason }
            reasons shouldContainExactly List(THREE) { DropReason.Detached }
            blocked.metrics.received shouldBe blocked.metrics.settled
        }

        test("reports what a cancelled publisher never handed to the consumers after the one it waited for") {
            val bus = devices()
            bus[Topics.devices].attach(INDEXER) {
                mailboxCapacity = 1
                handle { awaitCancellation() }
            }
            val recorder = EventRecorder<DeviceEvent>()
            val auditor = bus[Topics.devices].attach(AUDITOR) { handle(recorder::record) }
            listOf("r1", "r2", "r3").forEach { id ->
                bus[Topics.devices].send(DeviceEvent.Registered(id))
                testScheduler.runCurrent()
            }

            bus.close()

            recorder.envelopes.size shouldBe 2
            auditor.metrics.received shouldBe THREE
            auditor.metrics.dropped shouldBe 1
        }

        test("drops the retained envelopes it never reached when the bus closes") {
            val bus = bus { topic(Topics.devices) { retain = THREE } }
            listOf("r1", "r2", "r3").forEach { id -> bus[Topics.devices].send(DeviceEvent.Registered(id)) }
            testScheduler.runCurrent()
            val started = CompletableDeferred<Unit>()
            val consumer = bus[Topics.devices].attach(INDEXER) {
                start = StartPosition.Retained
                handle {
                    started.complete(Unit)
                    awaitCancellation()
                }
            }
            started.await()

            bus.close()

            consumer.metrics.received shouldBe THREE
            consumer.metrics.dropped shouldBe THREE
        }

        test("leaves nothing to drop when it is closed after it detached") {
            val bus = devices()
            val consumer = bus[Topics.devices].attach(INDEXER) { handle { } }
            consumer.detach()

            bus.close()

            consumer.state.value shouldBe ConsumerState.Detached
            bus[Topics.devices].hasConsumers shouldBe false
            bus[Topics.devices].toString() shouldBe "Topic(inventory.devices: DeviceEvent)"
        }
    }

    context("the dead letters").config(coroutineTestScope = true) {
        test("send nothing of their own anywhere") {
            val bus = devices()
            val envelope: Envelope<Event> = testEnvelope(DeviceEvent.Registered("r1"))
            val letter = DeadLetter(
                original = envelope,
                consumer = ConsumerName(INDEXER),
                attempts = 1,
                cause = Broken(UNINDEXABLE),
            )
            val sink: DeadLetterSink = bus.deadLetters.runtime.deadLetters

            sink.accept(letter, envelope) shouldBe DeadLetterOutcome.Closed
            letter.toString() shouldContain "cause=Broken"
        }
    }
})
