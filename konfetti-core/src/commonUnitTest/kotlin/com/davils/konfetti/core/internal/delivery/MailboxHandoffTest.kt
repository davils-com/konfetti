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

package com.davils.konfetti.core.internal.delivery

import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.testing.testEnvelope
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

private fun handoff(): MailboxHandoff<AuditEvent> = MailboxHandoff(testEnvelope(AuditEvent("handed")))

class MailboxHandoffTest : FunSpec({
    context("a pending handoff") {
        test("is placed by its put") {
            val pending = handoff()

            pending.confirm() shouldBe true
            pending.claim() shouldBe true
            pending.claim() shouldBe false
        }

        test("is left to its put when the channel gives it up while the put still runs") {
            val pending = handoff()

            pending.defer() shouldBe true
            pending.confirm() shouldBe false
            pending.claim() shouldBe true
        }

        test("needs nothing more from its put once a worker claimed it") {
            val pending = handoff()

            pending.claim() shouldBe true
            pending.confirm() shouldBe true
            pending.defer() shouldBe false
        }
    }

    context("a placed handoff") {
        test("is not deferred, so the channel settles it itself") {
            val placed = handoff()
            placed.confirm()

            placed.defer() shouldBe false
            placed.claim() shouldBe true
        }
    }
})
