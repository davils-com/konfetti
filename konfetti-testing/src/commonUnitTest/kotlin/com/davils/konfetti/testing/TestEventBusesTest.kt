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

import com.davils.konfetti.core.publish.PublishResult
import com.davils.konfetti.testing.fixture.Note
import com.davils.konfetti.testing.fixture.notes
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.backgroundScope
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlin.time.Instant

class TestEventBusesTest : FunSpec({
    context("a test bus").config(coroutineTestScope = true) {
        test("hands out identifiers that start at the epoch") {
            val bus = testEventBus(backgroundScope) { topic(notes) }

            val result = bus[notes].publish(Note("first"))

            val envelope = result.shouldBeInstanceOf<PublishResult.Published<Note>>().envelope
            envelope.occurredAt shouldBe Instant.fromEpochMilliseconds(0)
        }

        test("lets its block override what it sets") {
            val entropy = SeededEntropy(seed = 7)

            val bus = testEventBus(backgroundScope) {
                this.entropy = entropy
                topic(notes)
            }

            bus.topicNames shouldBe setOf(notes.name)
        }
    }
})
