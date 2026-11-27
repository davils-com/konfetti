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

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.publish.Rejection
import com.davils.konfetti.testing.testEnvelope
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.backgroundScope
import io.kotest.engine.coroutines.testScheduler
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlin.time.Duration.Companion.seconds

private const val RENDEZVOUS: Int = 0

private fun handoff(text: String): PartitionHandoff<AuditEvent> = PartitionHandoff(testEnvelope(AuditEvent(text)))

@OptIn(ExperimentalCoroutinesApi::class)
class PartitionTest : FunSpec({
    context("a send with a stall").config(coroutineTestScope = true) {
        test("is refused as an overflow once the stall passed and no pump took it") {
            val lost = mutableListOf<Envelope<AuditEvent>>()
            val partition = Partition<AuditEvent>(capacity = RENDEZVOUS) { envelope -> lost += envelope }
            val waiting = handoff("waiting")

            val sent = backgroundScope.async { partition.send(waiting, stall = 5.seconds) }
            testScheduler.advanceTimeBy(6.seconds)
            testScheduler.runCurrent()

            sent.await() shouldBe Rejection.Overflow
            waiting.take() shouldBe false
            lost shouldBe emptyList()
        }

        test("is handed on, not refused, when the pump took it just as the stall ran out") {
            val partition = Partition<AuditEvent>(capacity = RENDEZVOUS) { }
            val dispatched = mutableListOf<Envelope<AuditEvent>>()
            val waiting = handoff("taken")
            backgroundScope.launch {
                delay(5.seconds)
                partition.start(backgroundScope) { envelope -> dispatched += envelope }
            }

            val sent = backgroundScope.async { partition.send(waiting, stall = 5.seconds) }
            testScheduler.advanceTimeBy(6.seconds)
            testScheduler.runCurrent()

            sent.await() shouldBe null
            dispatched shouldBe listOf(waiting.envelope)
            waiting.abandon() shouldBe false
        }
    }

    context("a cancelled partition").config(coroutineTestScope = true) {
        test("counts an accepted handoff it still held as lost exactly once") {
            val lost = mutableListOf<Envelope<AuditEvent>>()
            val partition = Partition<AuditEvent>(capacity = 1) { envelope -> lost += envelope }
            val held = handoff("held")
            partition.trySend(held).isSuccess shouldBe true
            held.accept() shouldBe true

            partition.cancel()
            partition.cancel()

            lost shouldBe listOf(held.envelope)
        }

        test("refuses a handoff that arrives after the cancel") {
            val partition = Partition<AuditEvent>(capacity = 1) { }
            partition.cancel()
            val late = handoff("late")

            partition.send(late, stall = 5.seconds) shouldBe Rejection.Closed
            partition.trySend(handoff("offered")).isClosed shouldBe true
        }
    }

    context("a pump").config(coroutineTestScope = true) {
        test("skips a handoff its publisher abandoned before the pump took it") {
            val partition = Partition<AuditEvent>(capacity = 1) { }
            val dispatched = mutableListOf<Envelope<AuditEvent>>()
            val abandoned = handoff("abandoned")
            partition.trySend(abandoned)
            abandoned.abandon() shouldBe true
            val done = CompletableDeferred<Unit>()

            partition.close()
            partition.start(backgroundScope) { envelope -> dispatched += envelope }.invokeOnCompletion {
                done.complete(Unit)
            }
            done.await()

            dispatched shouldBe emptyList()
        }
    }
})
