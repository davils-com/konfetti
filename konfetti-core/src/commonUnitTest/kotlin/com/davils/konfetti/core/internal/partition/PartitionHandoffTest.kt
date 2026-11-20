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
import com.davils.konfetti.testing.testEnvelope
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

private fun handoff(): PartitionHandoff<AuditEvent> = PartitionHandoff(testEnvelope(AuditEvent("handed")))

class PartitionHandoffTest : FunSpec({
    context("a handoff the publisher accepted") {
        test("is taken by the pump once") {
            val sent = handoff()

            sent.accept() shouldBe true
            sent.take() shouldBe true
            sent.take() shouldBe false
            sent.undeliver() shouldBe false
        }

        test("is counted as lost when the channel gives it up before the pump took it") {
            val sent = handoff()
            sent.accept()

            sent.undeliver() shouldBe true
            sent.undeliver() shouldBe false
            sent.take() shouldBe false
        }
    }

    context("a handoff the pump took before the publisher accepted it") {
        test("is still accepted and cannot be abandoned") {
            val taken = handoff()

            taken.take() shouldBe true
            taken.accept() shouldBe true
            taken.abandon() shouldBe false
        }
    }

    context("a handoff the channel gave up before the publisher accepted it") {
        test("is neither accepted nor taken, and abandoning it agrees") {
            val lost = handoff()

            lost.undeliver() shouldBe false
            lost.accept() shouldBe false
            lost.take() shouldBe false
            lost.abandon() shouldBe true
        }
    }

    context("a handoff its publisher abandoned") {
        test("is never taken") {
            val abandoned = handoff()

            abandoned.abandon() shouldBe true
            abandoned.take() shouldBe false
        }
    }
})
