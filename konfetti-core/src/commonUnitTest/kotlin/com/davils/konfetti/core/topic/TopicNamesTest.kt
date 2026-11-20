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
import com.davils.konfetti.core.fixture.INDEXER
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class TopicNamesTest : FunSpec({
    context("topic names") {
        test("accept dotted lowercase names and render as their value") {
            TopicName("inventory.devices-changed").toString() shouldBe "inventory.devices-changed"
        }

        test("refuse anything else") {
            shouldThrow<IllegalArgumentException> { TopicName("Inventory") }
            shouldThrow<IllegalArgumentException> { TopicName("inventory..devices") }
            shouldThrow<IllegalArgumentException> { TopicName("") }
        }
    }

    context("consumer names") {
        test("follow the same rules") {
            ConsumerName(INDEXER).toString() shouldBe INDEXER
            shouldThrow<IllegalArgumentException> { ConsumerName("1indexer") }
        }
    }

    context("ordering keys") {
        test("render as their value and refuse an empty key") {
            OrderingKey("r1").toString() shouldBe "r1"
            shouldThrow<IllegalArgumentException> { OrderingKey("") }
        }
    }
})
