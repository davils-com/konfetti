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

package com.davils.konfetti.core.internal.platform

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class PlatformParityTest : FunSpec({
    context("a striped counter") {
        test("counts up and down from zero on every platform") {
            val counter = stripedCounter()

            repeat(5) { counter.increment() }
            counter.decrement()

            counter.sum shouldBe 4
        }
    }

    context("a guard") {
        test("returns what its block returns and may be entered again by its holder") {
            val guard = guard()

            guard.locked { guard.locked { 42 } } shouldBe 42
        }

        test("releases itself when its block throws") {
            val guard = guard()

            runCatching { guard.locked { error("thrown") } }

            guard.locked { "free" } shouldBe "free"
        }
    }
})
