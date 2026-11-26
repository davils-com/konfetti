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

package com.davils.konfetti.core.publish

import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.attach
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.testing.EventRecorder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldNotContain

private const val SECRET: String = "r-secret"

class EmitterTest : FunSpec({
    context("an emitter").config(coroutineTestScope = true) {
        test("declares its topic when the bus has none") {
            val bus = bus()

            val emitter = Emitter(bus, Topics.audits)

            emitter.isBound shouldBe true
            (Topics.audits in bus) shouldBe true
        }

        test("takes the topic the bus already declares") {
            val bus = devices()

            Emitter(bus, Topics.devices).isBound shouldBe true
        }

        test("builds no event while nobody listens") {
            val bus = devices()
            val emitter = Emitter(bus, Topics.devices)
            var built = 0

            emitter.emit {
                built += 1
                DeviceEvent.Registered("r1")
            } shouldBe false

            built shouldBe 0
            emitter.rejected shouldBe 0L
        }

        test("publishes what it builds to the consumers of the topic") {
            val bus = devices()
            val recorder = EventRecorder<DeviceEvent>()
            bus[Topics.devices].attach(INDEXER) { handle(recorder::record) }
            val emitter = Emitter(bus, Topics.devices)

            emitter.emit { DeviceEvent.Registered("r1") } shouldBe true

            recorder.awaitCount(1).single().payload shouldBe DeviceEvent.Registered("r1")
        }

        test("contains a failing builder and counts it") {
            val bus = devices()
            bus[Topics.devices].attach(INDEXER) { handle { } }
            val emitter = Emitter(bus, Topics.devices)

            emitter.emit { error("The event cannot be built") } shouldBe false

            emitter.rejected shouldBe 1L
        }

        test("counts an event the bus refuses") {
            val bus = devices()
            val emitter = Emitter(bus, Topics.devices)
            bus.close()

            emitter.send(DeviceEvent.Registered("r1")) shouldBe false

            emitter.rejected shouldBe 1L
        }

        test("stays unbound on a closed bus") {
            val bus = bus()
            bus.close()

            Emitter(bus, Topics.audits).isBound shouldBe false
        }

        test("drops every event without a bus") {
            val emitter = Emitter(null, Topics.devices)

            emitter.isBound shouldBe false
            emitter.hasConsumers shouldBe false
            emitter.send(DeviceEvent.Registered("r1")) shouldBe false
        }

        test("stays unbound when the bus declares the name for another event type") {
            val bus = devices()

            val emitter = Emitter(bus, Topics.impostor)

            emitter.isBound shouldBe false
            emitter.send(AuditEvent("entry")) shouldBe false
        }

        test("shows its topic but never an event") {
            val emitter = Emitter(null, Topics.devices)
            emitter.send(DeviceEvent.Registered(SECRET))

            emitter.toString() shouldBe "Emitter(topic=${Topics.devices.name}, bound=false)"
            emitter.toString() shouldNotContain SECRET
        }
    }
})
