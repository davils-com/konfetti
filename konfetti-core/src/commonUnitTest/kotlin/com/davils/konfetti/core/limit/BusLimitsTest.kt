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

package com.davils.konfetti.core.limit

import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.kore.core.validation.report.ValidationException
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours

private const val DEFAULT_TOPICS: Int = 1_024
private const val DEFAULT_CONSUMERS: Int = 256
private const val DEFAULT_ATTEMPTS: Int = 100
private const val DEFAULT_HEADERS: Int = 64
private const val DEFAULT_HEADER_VALUE_LENGTH: Int = 4_096
private const val DEFAULT_KEY_LENGTH: Int = 1_024
private const val BASE_HEADERS: Int = 8
private const val CHANGED_TOPICS: Int = 16
private const val LIMITS: Int = 12
private const val NOT_POSITIVE_LIMITS: Int = 4

class BusLimitsTest : FunSpec({
    context("the default limits") {
        test("bound every structure of a bus") {
            val limits = BusLimits.Default

            limits.maximumTopics shouldBe DEFAULT_TOPICS
            limits.maximumConsumers shouldBe DEFAULT_CONSUMERS
            limits.maximumPartitions shouldBe BusLimits.HARD_PARTITIONS
            limits.maximumBufferCapacity shouldBe BusLimits.HARD_BUFFER_CAPACITY
            limits.maximumMailboxCapacity shouldBe BusLimits.HARD_MAILBOX_CAPACITY
            limits.maximumRetain shouldBe BusLimits.HARD_RETAIN
            limits.maximumAttempts shouldBe DEFAULT_ATTEMPTS
            limits.maximumHeaders shouldBe DEFAULT_HEADERS
            limits.maximumHeaderValueLength shouldBe DEFAULT_HEADER_VALUE_LENGTH
            limits.maximumOrderingKeyLength shouldBe DEFAULT_KEY_LENGTH
            limits.maximumHandlingTimeout shouldBe 1.days
            limits.maximumRetryDelay shouldBe 1.hours
        }

        test("compare, hash and show by value") {
            val same = busLimits { }
            val other = busLimits { maximumTopics = 2 }

            same shouldBe BusLimits.Default
            same.hashCode() shouldBe BusLimits.Default.hashCode()
            other shouldNotBe BusLimits.Default
            busLimits { maximumRetryDelay = 2.hours } shouldNotBe BusLimits.Default
            BusLimits.Default.equals("limits") shouldBe false
            other.toString() shouldContain "topics=2"
        }
    }

    context("built limits") {
        test("start from their base and change what the block sets") {
            val base = busLimits { maximumHeaders = BASE_HEADERS }

            val limits = busLimits(base) { maximumTopics = CHANGED_TOPICS }

            limits.maximumHeaders shouldBe BASE_HEADERS
            limits.maximumTopics shouldBe CHANGED_TOPICS
        }

        test("accept every limit at its hard ceiling") {
            val limits = busLimits {
                maximumTopics = BusLimits.HARD_TOPICS
                maximumConsumers = BusLimits.HARD_CONSUMERS
                maximumAttempts = BusLimits.HARD_ATTEMPTS
                maximumHeaders = BusLimits.HARD_HEADERS
                maximumHeaderValueLength = BusLimits.HARD_HEADER_VALUE_LENGTH
                maximumOrderingKeyLength = BusLimits.HARD_ORDERING_KEY_LENGTH
                maximumHandlingTimeout = BusLimits.HARD_HANDLING_TIMEOUT
                maximumRetryDelay = BusLimits.HARD_RETRY_DELAY
            }

            limits.maximumTopics shouldBe BusLimits.HARD_TOPICS
        }

        test("refuse every count one beyond its hard ceiling") {
            val failure = shouldThrow<ValidationException> {
                busLimits {
                    maximumTopics = BusLimits.HARD_TOPICS + 1
                    maximumConsumers = BusLimits.HARD_CONSUMERS + 1
                    maximumPartitions = BusLimits.HARD_PARTITIONS + 1
                    maximumBufferCapacity = BusLimits.HARD_BUFFER_CAPACITY + 1
                    maximumMailboxCapacity = BusLimits.HARD_MAILBOX_CAPACITY + 1
                    maximumRetain = BusLimits.HARD_RETAIN + 1
                    maximumAttempts = BusLimits.HARD_ATTEMPTS + 1
                    maximumHeaders = BusLimits.HARD_HEADERS + 1
                    maximumHeaderValueLength = BusLimits.HARD_HEADER_VALUE_LENGTH + 1
                    maximumOrderingKeyLength = BusLimits.HARD_ORDERING_KEY_LENGTH + 1
                    maximumHandlingTimeout = BusLimits.HARD_HANDLING_TIMEOUT + 1.hours
                    maximumRetryDelay = BusLimits.HARD_RETRY_DELAY + 1.hours
                }
            }

            val codes = failure.report.errors.map { violation -> violation.code }
            codes.toSet() shouldContainExactly setOf(EventViolationCodes.ABOVE_HARD_LIMIT)
            codes.size shouldBe LIMITS
        }

        test("refuse a count below one and a duration that is not positive") {
            val failure = shouldThrow<ValidationException> {
                busLimits {
                    maximumTopics = 0
                    maximumHeaders = -1
                    maximumHandlingTimeout = Duration.ZERO
                    maximumRetryDelay = -1.hours
                }
            }

            val codes = failure.report.errors.map { violation -> violation.code }
            codes.toSet() shouldContainExactly setOf(EventViolationCodes.NOT_POSITIVE)
            codes.size shouldBe NOT_POSITIVE_LIMITS
        }

        test("cannot be changed once they were built") {
            val builder = BusLimitsBuilder()
            builder.build()

            shouldThrow<IllegalStateException> { builder.maximumTopics = 1 }
            shouldThrow<IllegalStateException> { builder.maximumConsumers = 1 }
            shouldThrow<IllegalStateException> { builder.maximumPartitions = 1 }
            shouldThrow<IllegalStateException> { builder.maximumBufferCapacity = 1 }
            shouldThrow<IllegalStateException> { builder.maximumMailboxCapacity = 1 }
            shouldThrow<IllegalStateException> { builder.maximumRetain = 1 }
            shouldThrow<IllegalStateException> { builder.maximumAttempts = 1 }
            shouldThrow<IllegalStateException> { builder.maximumHeaders = 1 }
            shouldThrow<IllegalStateException> { builder.maximumHeaderValueLength = 1 }
            shouldThrow<IllegalStateException> { builder.maximumOrderingKeyLength = 1 }
            shouldThrow<IllegalStateException> { builder.maximumHandlingTimeout = 1.hours }
            shouldThrow<IllegalStateException> { builder.maximumRetryDelay = 1.hours }
        }
    }
})
