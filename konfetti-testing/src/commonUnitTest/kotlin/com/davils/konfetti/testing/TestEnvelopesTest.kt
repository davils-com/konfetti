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

package com.davils.konfetti.testing

import com.davils.konfetti.core.header.HeaderKey
import com.davils.konfetti.core.header.headersOf
import com.davils.konfetti.core.topic.OrderingKey
import com.davils.konfetti.core.topic.TopicName
import com.davils.konfetti.testing.fixture.Note
import com.davils.kore.core.principal.Principal
import com.davils.kore.testing.SequentialUuidV7Generator
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class TestEnvelopesTest : FunSpec({
    context("a test envelope") {
        test("defaults to a test topic, no key, no principal and no cause") {
            val envelope = testEnvelope(Note("created"))

            envelope.topic shouldBe TopicName("test.topic")
            envelope.orderingKey shouldBe null
            envelope.principal shouldBe null
            envelope.headers.isEmpty shouldBe true
            envelope.correlationId shouldBe envelope.id
            envelope.isCaused shouldBe false
        }

        test("carries what its builder sets") {
            val generator = SequentialUuidV7Generator()
            val cause = testEnvelope(Note("cause")) { identifiers = generator }
            val trace = HeaderKey.text("trace-id")

            val envelope = testEnvelope(Note("effect")) {
                topic = TopicName("audit.entries")
                orderingKey = OrderingKey("r1")
                headers = headersOf(trace("t-1"))
                principal = Principal("alice")
                causedBy = cause
                identifiers = generator
            }

            envelope.topic shouldBe TopicName("audit.entries")
            envelope.orderingKey shouldBe OrderingKey("r1")
            envelope.headers[trace] shouldBe "t-1"
            envelope.principal shouldBe Principal("alice")
            envelope.causationId shouldBe cause.id
            envelope.correlationId shouldBe cause.id
        }
    }
})
