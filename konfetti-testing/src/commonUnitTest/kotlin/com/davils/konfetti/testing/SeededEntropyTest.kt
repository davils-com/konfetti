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

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain

private const val DRAWS: Int = 1_000

class SeededEntropyTest : FunSpec({
    context("seeded entropy") {
        test("repeats its sequence for the same seed") {
            val first = SeededEntropy(seed = 42)
            val second = SeededEntropy(seed = 42)

            List(DRAWS) { first.nextLong() } shouldBe List(DRAWS) { second.nextLong() }
        }

        test("follows SplitMix64") {
            SeededEntropy(seed = 0).nextLong() shouldBe -0x1DDF57C684E23251
        }

        test("differs between seeds and does not repeat soon") {
            val entropy = SeededEntropy(seed = 1)
            val draws = List(DRAWS) { entropy.nextLong() }

            SeededEntropy(seed = 1).nextLong() shouldNotBe SeededEntropy(seed = 2).nextLong()
            draws.toSet() shouldHaveSize DRAWS
        }

        test("shows its seed") {
            SeededEntropy(seed = 9).toString() shouldContain "seed=9"
        }
    }
})
