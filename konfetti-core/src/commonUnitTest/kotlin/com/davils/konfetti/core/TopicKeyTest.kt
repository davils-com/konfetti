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

package com.davils.konfetti.core

import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.fixture.DEVICES
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.topic.TopicName
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

class TopicKeyTest : FunSpec({
    context("declaration") {
        test("carries the name and the payload type") {
            Topics.devices.name shouldBe TopicName(DEVICES)
            Topics.devices.eventType shouldBe DeviceEvent::class
        }

        test("refuses a name that is no topic name") {
            shouldThrow<IllegalArgumentException> { TopicKey<AuditEvent>("Not A Topic") }
        }
    }

    context("equality") {
        test("treats keys with the same name and type as equal") {
            TopicKey<DeviceEvent>(DEVICES) shouldBe Topics.devices
            TopicKey<DeviceEvent>(DEVICES).hashCode() shouldBe Topics.devices.hashCode()
        }

        test("tells keys with the same name but another type apart") {
            Topics.impostor shouldNotBe Topics.devices
        }

        test("is never equal to something that is no key") {
            Topics.devices.equals(Topics.devices.name) shouldBe false
        }
    }

    context("rendering") {
        test("shows the name and the payload type") {
            Topics.devices.toString() shouldBe "TopicKey(inventory.devices: DeviceEvent)"
        }
    }
})
