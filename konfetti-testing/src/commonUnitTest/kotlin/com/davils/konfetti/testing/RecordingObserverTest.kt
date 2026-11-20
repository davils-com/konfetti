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

import com.davils.konfetti.core.lifecycle.BusState
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.testing.fixture.notes
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.backgroundScope
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain

class RecordingObserverTest : FunSpec({
    context("a recording observer").config(coroutineTestScope = true) {
        test("records observations in order and filters them by type") {
            val observer = RecordingObserver()
            val bus = testEventBus(backgroundScope) {
                observe(observer)
                topic(notes)
            }

            bus.close()

            observer.only<BusObservation.StateChanged>().map { changed -> changed.state } shouldContainExactly
                listOf(BusState.Closed)
            observer.count<BusObservation.TopicDeclared>() shouldBe 1
            observer.observations.size shouldBe 2
            observer.isObserving shouldBe true
        }

        test("keeps at most its capacity and forgets everything when cleared") {
            val observer = RecordingObserver(capacity = 1)
            val bus = testEventBus(backgroundScope) {
                observe(observer)
                topic(notes)
            }

            bus.close()

            observer.observations.size shouldBe 1
            observer.overflow shouldBe 1
            observer.toString() shouldContain "recorded=1, overflow=1"
            observer.clear()
            observer.observations shouldBe emptyList()
            observer.overflow shouldBe 0
        }

        test("refuses a capacity outside its bounds") {
            shouldThrow<IllegalArgumentException> { RecordingObserver(capacity = 0) }
        }
    }
})
