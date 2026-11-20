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

import com.davils.konfetti.core.consume.OverflowPolicy
import com.davils.konfetti.core.consume.StartPosition
import com.davils.konfetti.core.failure.exponential
import com.davils.konfetti.core.failure.retry
import com.davils.konfetti.core.header.HeaderKey
import com.davils.konfetti.core.header.headersOf
import com.davils.konfetti.core.lifecycle.DrainResult
import com.davils.konfetti.core.publish.PublishResult
import com.davils.konfetti.testing.fixture.Note
import com.davils.konfetti.testing.fixture.notes
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.backgroundScope
import io.kotest.engine.coroutines.testScheduler
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private const val PARTITIONS: Int = 8
private const val RETAINED: Int = 100

class DocumentationTest : FunSpec({
    context("the guide in docs/events.md").config(coroutineTestScope = true) {
        test("declares, consumes, publishes and drains as it shows") {
            val recorder = EventRecorder<Note>()
            val trace = HeaderKey.text("trace-id")
            val bus = testEventBus(backgroundScope) {
                timeSource = testScheduler.timeSource
                topic(notes) {
                    partitions = PARTITIONS
                    retain = RETAINED
                    orderBy { note -> note.text }
                }
            }
            val consumption = bus[notes].consume("test.indexer") {
                concurrency = PARTITIONS
                start = StartPosition.Retained
                overflow = OverflowPolicy.Backpressure
                maximumStall = 5.seconds
                handlingTimeout = 30.seconds
                onFailure = retry(
                    attempts = 3,
                    backoff = exponential(initial = 100.milliseconds, max = 5.seconds, jitter = true),
                )
                where { envelope -> envelope.headers[trace] != null }
                handle(recorder::record)
            }
            consumption.getOrThrow()

            val result = bus[notes].publish(Note("r1"), headers = headersOf(trace("t-1")))

            result.shouldBeInstanceOf<PublishResult.Published<Note>>()
            recorder.awaitCount(1).single().payload shouldBe Note("r1")
            bus.drain(30.seconds) shouldBe DrainResult.Completed
        }
    }
})
