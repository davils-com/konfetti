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

import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.attach
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.fixture.send
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.core.publish.PublishResult
import com.davils.konfetti.core.publish.Rejection
import com.davils.konfetti.testing.EventRecorder
import com.davils.konfetti.testing.RecordingObserver
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.testScheduler
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.CompletableDeferred

private const val DELIVERED: Int = 4

class OverflowTest : FunSpec({
    context("a consumer that falls behind").config(coroutineTestScope = true) {
        test("drops the newest envelope when it drops the latest") {
            val observer = RecordingObserver()
            val bus = bus {
                observe(observer)
                topic(Topics.devices)
            }
            val gate = CompletableDeferred<Unit>()
            val recorder = EventRecorder<DeviceEvent>()
            val consumer = bus[Topics.devices].attach(INDEXER) {
                mailboxCapacity = 1
                overflow = OverflowPolicy.DropLatest
                handle { envelope ->
                    gate.await()
                    recorder.record(envelope)
                }
            }

            listOf("r1", "r2", "r3").forEach { id ->
                bus[Topics.devices].send(DeviceEvent.Registered(id))
                testScheduler.runCurrent()
            }
            gate.complete(Unit)

            recorder.awaitCount(2).map { envelope -> envelope.payload.id } shouldContainExactly listOf("r1", "r2")
            val dropped = observer.only<BusObservation.Dropped>().single()
            dropped.envelope.payload shouldBe DeviceEvent.Registered("r3")
            dropped.reason shouldBe DropReason.Overflow
            dropped.consumer shouldBe consumer.name
            consumer.metrics.dropped shouldBe 1
        }

        test("drops the oldest waiting envelope when it drops the oldest") {
            val observer = RecordingObserver()
            val bus = bus {
                observe(observer)
                topic(Topics.devices)
            }
            val gate = CompletableDeferred<Unit>()
            val recorder = EventRecorder<DeviceEvent>()
            bus[Topics.devices].attach(INDEXER) {
                mailboxCapacity = 1
                overflow = OverflowPolicy.DropOldest
                handle { envelope ->
                    gate.await()
                    recorder.record(envelope)
                }
            }

            listOf("r1", "r2", "r3").forEach { id ->
                bus[Topics.devices].send(DeviceEvent.Registered(id))
                testScheduler.runCurrent()
            }
            gate.complete(Unit)

            recorder.awaitCount(2).map { envelope -> envelope.payload.id } shouldContainExactly listOf("r1", "r3")
            val dropped = observer.only<BusObservation.Dropped>().single()
            dropped.envelope.payload shouldBe DeviceEvent.Registered("r2")
        }

        test("holds back publishers and loses nothing when it applies backpressure") {
            val bus = bus { topic(Topics.devices) { bufferCapacity = 1 } }
            val gate = CompletableDeferred<Unit>()
            val recorder = EventRecorder<DeviceEvent>()
            val topic = bus[Topics.devices]
            topic.attach(INDEXER) {
                mailboxCapacity = 1
                handle { envelope ->
                    gate.await()
                    recorder.record(envelope)
                }
            }

            listOf("r1", "r2", "r3").forEach { id ->
                topic.send(DeviceEvent.Registered(id))
                testScheduler.runCurrent()
            }
            val fourth = topic.tryPublish(DeviceEvent.Registered("r4"))
            val fifth = topic.tryPublish(DeviceEvent.Registered("r5"))
            gate.complete(Unit)

            fourth.shouldBeInstanceOf<PublishResult.Published<DeviceEvent>>()
            fifth shouldBe PublishResult.Rejected(Topics.devices.name, Rejection.Overflow)
            recorder.awaitCount(DELIVERED).map { envelope -> envelope.payload.id } shouldContainExactly
                listOf("r1", "r2", "r3", "r4")
        }
    }
})
