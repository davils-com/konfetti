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

import com.davils.konfetti.core.fixture.ALICE
import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.header.HeaderKey
import com.davils.konfetti.core.header.headersOf
import com.davils.konfetti.core.topic.OrderingKey
import com.davils.konfetti.testing.testEnvelope
import com.davils.kore.core.principal.Principal
import com.davils.kore.testing.SequentialUuidV7Generator
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import kotlin.time.Instant

private val secret = HeaderKey.text("api-token", sensitive = true)

class EnvelopeTest : FunSpec({
    context("identity") {
        test("reports when it occurred from its identifier") {
            val envelope = testEnvelope(AuditEvent("created"))

            envelope.occurredAt shouldBe Instant.fromEpochMilliseconds(0)
        }

        test("is equal to an envelope with the same identifier only") {
            val first = testEnvelope(AuditEvent("one"))
            val sameId = testEnvelope(AuditEvent("two"))
            val generator = SequentialUuidV7Generator()
            generator.next()
            val other = testEnvelope(AuditEvent("one")) { identifiers = generator }

            first shouldBe sameId
            first.hashCode() shouldBe sameId.hashCode()
            first shouldNotBe other
            first.equals(first.payload) shouldBe false
        }

        test("is caused when it names a cause") {
            val generator = SequentialUuidV7Generator()
            val cause = testEnvelope(AuditEvent("cause")) { identifiers = generator }
            val effect = testEnvelope(AuditEvent("effect")) {
                causedBy = cause
                identifiers = generator
            }

            cause.isCaused shouldBe false
            effect.isCaused shouldBe true
            effect.causationId shouldBe cause.id
            effect.correlationId shouldBe cause.correlationId
        }
    }

    context("rendering") {
        test("names the type of the payload but never its content") {
            val rendered = testEnvelope(DeviceEvent.Renamed(id = "r1", name = "core-switch")).toString()

            rendered shouldContain "payload=Renamed"
            rendered shouldNotContain "core-switch"
        }

        test("shows the ordering key, the cause, masked headers and the id of the principal") {
            val generator = SequentialUuidV7Generator()
            val cause = testEnvelope(AuditEvent("cause")) { identifiers = generator }
            val envelope = testEnvelope(AuditEvent("effect")) {
                orderingKey = OrderingKey("r1")
                headers = headersOf(secret("hunter2"))
                principal = Principal(ALICE)
                causedBy = cause
                identifiers = generator
            }
            val rendered = envelope.toString()

            rendered shouldContain "orderingKey=r1"
            rendered shouldContain "causationId=${cause.id}"
            rendered shouldContain "headers={api-token=****}"
            rendered shouldContain "principal=alice"
            rendered shouldNotContain "hunter2"
        }

        test("names an anonymous payload as such") {
            val anonymous = object : Event {}

            testEnvelope(anonymous).toString() shouldContain "payload=<anonymous>"
        }
    }
})
