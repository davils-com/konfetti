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

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.consume.OverflowPolicy
import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.testing.testEnvelope
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.backgroundScope
import io.kotest.engine.coroutines.testScheduler
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

private const val FIRST: String = "first"
private const val SECOND: String = "second"

private fun recordingMailbox(
    overflow: OverflowPolicy,
    lost: MutableList<DropReason>,
    stall: Duration = Duration.INFINITE,
): Mailbox<AuditEvent> = Mailbox(
    capacity = 1,
    overflow = overflow,
    maximumStall = stall,
    lost = { _: Envelope<AuditEvent>, reason -> lost += reason },
    reason = { DropReason.Detached },
)

private fun audit(text: String): Envelope<AuditEvent> = testEnvelope(AuditEvent(text))

@OptIn(ExperimentalCoroutinesApi::class)
class MailboxTest : FunSpec({
    context("a closed mailbox") {
        test("drops under the reason it was closed for, whatever its policy") {
            OverflowPolicy.entries.forEach { overflow ->
                val lost = mutableListOf<DropReason>()
                val mailbox = recordingMailbox(overflow, lost)
                mailbox.close()

                mailbox.put(audit("late"))

                lost shouldBe listOf(DropReason.Detached)
            }
        }

        test("still hands what it holds to its consumer") {
            val lost = mutableListOf<DropReason>()
            val mailbox = recordingMailbox(OverflowPolicy.Backpressure, lost)
            val envelope = audit("kept")
            mailbox.put(envelope)
            mailbox.close()
            val handled = mutableListOf<Envelope<AuditEvent>>()

            mailbox.consume { delivered -> handled += delivered }

            handled shouldBe listOf(envelope)
            lost shouldBe emptyList()
        }
    }

    context("a cancelled mailbox").config(coroutineTestScope = true) {
        test("counts a put that was waiting for room exactly once") {
            val lost = mutableListOf<DropReason>()
            val mailbox = recordingMailbox(OverflowPolicy.Backpressure, lost)
            mailbox.put(audit(FIRST))
            val waiting = backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) { mailbox.put(audit(SECOND)) }

            waiting.cancelAndJoin()
            mailbox.cancel()

            lost shouldBe listOf(DropReason.Detached, DropReason.Detached)
        }

        test("counts a put that arrives after the cancel exactly once") {
            OverflowPolicy.entries.forEach { overflow ->
                val lost = mutableListOf<DropReason>()
                val mailbox = recordingMailbox(overflow, lost)
                mailbox.cancel()

                mailbox.put(audit("late"))

                lost shouldBe listOf(DropReason.Detached)
            }
        }

        test("counts what it held exactly once") {
            val lost = mutableListOf<DropReason>()
            val mailbox = recordingMailbox(OverflowPolicy.DropLatest, lost)
            mailbox.put(audit("held"))

            mailbox.cancel()
            mailbox.cancel()

            lost shouldBe listOf(DropReason.Detached)
        }
    }

    context("a full mailbox").config(coroutineTestScope = true) {
        test("drops the new envelope under DropLatest") {
            val lost = mutableListOf<DropReason>()
            val mailbox = recordingMailbox(OverflowPolicy.DropLatest, lost)
            mailbox.put(audit(FIRST))

            mailbox.put(audit(SECOND))

            lost shouldBe listOf(DropReason.Overflow)
        }

        test("drops the oldest envelope under DropOldest") {
            val lost = mutableListOf<DropReason>()
            val mailbox = recordingMailbox(OverflowPolicy.DropOldest, lost)
            mailbox.put(audit(FIRST))
            val second = audit(SECOND)

            mailbox.put(second)
            mailbox.close()
            val handled = mutableListOf<Envelope<AuditEvent>>()
            mailbox.consume { delivered -> handled += delivered }

            lost shouldBe listOf(DropReason.Overflow)
            handled shouldBe listOf(second)
        }

        test("drops an envelope as stalled once the longest stall passed under Backpressure") {
            val lost = mutableListOf<DropReason>()
            val mailbox = recordingMailbox(OverflowPolicy.Backpressure, lost, stall = 5.seconds)
            mailbox.put(audit(FIRST))
            val waiting = backgroundScope.launch { mailbox.put(audit(SECOND)) }

            testScheduler.advanceTimeBy(4.seconds)
            testScheduler.runCurrent()
            lost shouldBe emptyList()
            testScheduler.advanceTimeBy(2.seconds)
            testScheduler.runCurrent()

            waiting.isCompleted shouldBe true
            lost shouldBe listOf(DropReason.Stalled)
        }

        test("hands an envelope on that found room before the longest stall passed") {
            val lost = mutableListOf<DropReason>()
            val mailbox = recordingMailbox(OverflowPolicy.Backpressure, lost, stall = 5.seconds)
            mailbox.put(audit(FIRST))
            val handled = mutableListOf<Envelope<AuditEvent>>()
            val waiting = backgroundScope.launch { mailbox.put(audit(SECOND)) }
            testScheduler.advanceTimeBy(1.seconds)
            testScheduler.runCurrent()

            backgroundScope.launch { mailbox.consume { delivered -> handled += delivered } }
            testScheduler.runCurrent()

            waiting.isCompleted shouldBe true
            handled.map { envelope -> envelope.payload.text } shouldBe listOf(FIRST, SECOND)
            lost shouldBe emptyList()
        }
    }
})
