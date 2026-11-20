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

package com.davils.konfetti.core.topic

import com.davils.konfetti.core.consume.ConsumerName
import com.davils.konfetti.core.limit.BusLimits
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class OrderingKeyTest : FunSpec({
    context("an ordering key") {
        test("is neither empty nor longer than the hard length") {
            OrderingKey("x".repeat(BusLimits.HARD_ORDERING_KEY_LENGTH)).value.length shouldBe
                BusLimits.HARD_ORDERING_KEY_LENGTH
            shouldThrow<IllegalArgumentException> { OrderingKey("") }
            shouldThrow<IllegalArgumentException> { OrderingKey("x".repeat(BusLimits.HARD_ORDERING_KEY_LENGTH + 1)) }
            OrderingKey("r1").toString() shouldBe "r1"
        }
    }

    context("names") {
        test("tell whether a text follows their grammar and sort by their text") {
            TopicName.isValid("inventory.devices") shouldBe true
            TopicName.isValid("Inventory") shouldBe false
            TopicName.isValid("a".repeat(TopicName.MAX_LENGTH + 1)) shouldBe false
            ConsumerName.isValid("inventory.indexer") shouldBe true
            ConsumerName.isValid("-indexer") shouldBe false
            (ConsumerName("b") > ConsumerName("a")) shouldBe true
            (TopicName("b") > TopicName("a")) shouldBe true
        }
    }
})
