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

import com.davils.konfetti.testing.fixture.Note
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.backgroundScope
import io.kotest.engine.coroutines.testScheduler
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async

private const val FIRST: String = "first"
private const val SECOND: String = "second"
private const val RECORDED: Int = 5

@OptIn(ExperimentalCoroutinesApi::class)
class EventRecorderTest : FunSpec({
    context("a recorder").config(coroutineTestScope = true) {
        test("keeps envelopes and payloads in the order they arrived") {
            val recorder = EventRecorder<Note>()
            val first = testEnvelope(Note(FIRST))
            val second = testEnvelope(Note(SECOND))

            recorder.record(first)
            recorder.record(second)

            recorder.envelopes shouldBe listOf(first, second)
            recorder.payloads shouldBe listOf(Note(FIRST), Note(SECOND))
            recorder.awaitCount(2) shouldBe listOf(first, second)
        }

        test("waits until enough envelopes arrived") {
            val recorder = EventRecorder<Note>()
            val waiting = backgroundScope.async { recorder.awaitCount(2) }
            recorder.record(testEnvelope(Note(FIRST)))
            testScheduler.runCurrent()

            waiting.isCompleted shouldBe false
            recorder.record(testEnvelope(Note(SECOND)))

            waiting.await().size shouldBe 2
        }

        test("keeps at most its capacity and counts the rest") {
            val recorder = EventRecorder<Note>(capacity = 2)

            repeat(RECORDED) { index -> recorder.record(testEnvelope(Note("n$index"))) }

            recorder.payloads shouldBe listOf(Note("n0"), Note("n1"))
            recorder.overflow shouldBe RECORDED - 2L
            recorder.toString() shouldContain "recorded=2, overflow=3"
        }

        test("refuses a capacity outside its bounds and a count it could never hold") {
            shouldThrow<IllegalArgumentException> { EventRecorder<Note>(capacity = 0) }
            shouldThrow<IllegalArgumentException> { EventRecorder<Note>(capacity = EventRecorder.MAX_CAPACITY + 1) }
            shouldThrow<IllegalArgumentException> { EventRecorder<Note>(capacity = 1).awaitCount(2) }
        }
    }
})
