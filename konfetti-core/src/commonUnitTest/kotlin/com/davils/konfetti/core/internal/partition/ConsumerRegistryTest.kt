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

package com.davils.konfetti.core.internal.partition

import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.topic.TopicName
import com.davils.konfetti.testing.testEnvelope
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe

class ConsumerRegistryTest : FunSpec({
    context("a registry that retains") {
        test("records nothing once its topic is closed") {
            val registry = ConsumerRegistry<AuditEvent>(TopicName("audit.entries"), retain = 2, limit = 4)

            registry.record(0, testEnvelope(AuditEvent("late"))) { false } shouldBe null
            registry.retained shouldBe 0
        }
    }

    context("a retention") {
        test("keeps the last envelopes up to its capacity") {
            val retention = Retention<AuditEvent>(capacity = 2)

            listOf("a", "b", "c").forEachIndexed { index, text -> retention.add(index, testEnvelope(AuditEvent(text))) }

            retention.snapshot().map { retained -> retained.envelope.payload.text } shouldContainExactly listOf("b", "c")
            retention.size shouldBe 2
        }

        test("keeps nothing without a capacity") {
            val retention = Retention<AuditEvent>(capacity = 0)

            retention.add(0, testEnvelope(AuditEvent("a")))

            retention.isDisabled shouldBe true
            retention.size shouldBe 0
        }
    }
})
