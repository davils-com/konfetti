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

package com.davils.konfetti.core.observe

import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.attach
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.core.fixture.send
import com.davils.konfetti.core.lifecycle.BusState
import com.davils.konfetti.testing.EventRecorder
import com.davils.konfetti.testing.RecordingObserver
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.backgroundScope
import io.kotest.engine.coroutines.testScheduler
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList

private const val BURST: Int = 5
private const val PARTITIONS: Int = 3

private val silent = object : BusObserver {
    override val isObserving: Boolean get() = false

    override fun observe(observation: BusObservation): Unit = error("must not be called")
}

class ObservationTest : FunSpec({
    context("observers").config(coroutineTestScope = true) {
        test("are isolated from the bus, which counts their failures and carries on") {
            val recorder = EventRecorder<DeviceEvent>()
            val bus = devices { observe { error("broken observer") } }
            bus[Topics.devices].attach(INDEXER) { handle(recorder::record) }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            recorder.awaitCount(1)
            (bus.snapshot().observerFailures > 0) shouldBe true
        }

        test("are not called while they do not observe") {
            val bus = devices { observe(silent) }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            bus.snapshot().observerFailures shouldBe 0
        }

        test("include one that observes nothing") {
            BusObserver.None.isObserving shouldBe false
            BusObserver.None.observe(BusObservation.StateChanged(BusState.Running))
        }

        test("can be recorded and cleared") {
            val observer = RecordingObserver()
            val bus = devices { observe(observer) }

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            observer.count<BusObservation.Published>() shouldBe 1
            observer.observations.isEmpty() shouldBe false
            observer.clear()
            observer.observations shouldBe emptyList()
        }
    }

    context("snapshots").config(coroutineTestScope = true) {
        test("show every topic, the dead letters last, with their consumers") {
            val bus = devices()
            val recorder = EventRecorder<DeviceEvent>()
            val consumer = bus[Topics.devices].attach(INDEXER) { handle(recorder::record) }
            bus[Topics.devices].send(DeviceEvent.Registered("r1"))
            recorder.awaitCount(1)
            testScheduler.runCurrent()

            val snapshot = bus.snapshot()

            snapshot.state shouldBe BusState.Running
            snapshot.topics.map { topic -> topic.name } shouldContainExactly listOf(
                Topics.devices.name,
                bus.deadLetters.name,
            )
            val devices = snapshot.topics.first()
            devices.state shouldBe BusState.Running
            devices.partitions shouldBe 1
            devices.metrics.published shouldBe 1
            devices.metrics.dispatched shouldBe 1
            devices.metrics.queued shouldBe 0
            devices.consumers.getValue(consumer.name).delivered shouldBe 1
        }

        test("show how many partitions a topic spreads its envelopes over") {
            val bus = bus {
                topic(Topics.devices) {
                    partitions = PARTITIONS
                    orderBy { event -> event.id }
                }
            }

            val topics = bus.snapshot().topics

            topics.first().partitions shouldBe PARTITIONS
            topics.last().partitions shouldBe 1
        }
    }

    context("streams").config(coroutineTestScope = true) {
        test("hand observations to whoever collects them") {
            val stream = ObservationStream()
            val bus = devices { observe(stream) }
            stream.isObserving shouldBe false
            val observations = stream.observations()
            val collected = backgroundScope.async { observations.take(1).toList() }
            testScheduler.runCurrent()

            bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            val first = collected.await().single()
            first.shouldBeObservation<BusObservation.Published>()
        }

        test("count what a slow collector missed") {
            val stream = ObservationStream(capacity = 1)
            val sequenced = stream.sequenced()
            val collected = backgroundScope.async { sequenced.take(2).toList() }
            testScheduler.runCurrent()

            repeat(BURST) { stream.observe(BusObservation.StateChanged(BusState.Running)) }
            testScheduler.runCurrent()
            stream.observe(BusObservation.StateChanged(BusState.Closed))

            val received = collected.await()
            received.size shouldBe 2
            received.sumOf { observation -> observation.missed } shouldBe received.last().sequence - 2
        }

        test("ignore what nobody collects") {
            val stream = ObservationStream()

            stream.observe(BusObservation.StateChanged(BusState.Running))
            val sequenced = stream.sequenced()
            val first = backgroundScope.async { sequenced.take(1).toList() }
            testScheduler.runCurrent()
            stream.observe(BusObservation.StateChanged(BusState.Closed))

            first.await().single().sequence shouldBe 1
        }

        test("refuse a capacity below one") {
            shouldThrow<IllegalArgumentException> { ObservationStream(capacity = 0) }
        }
    }
})

private inline fun <reified O : BusObservation> BusObservation.shouldBeObservation() {
    (this is O) shouldBe true
}
