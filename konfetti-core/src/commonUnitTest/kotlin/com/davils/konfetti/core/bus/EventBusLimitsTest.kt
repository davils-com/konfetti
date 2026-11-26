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

package com.davils.konfetti.core.bus

import com.davils.konfetti.core.consume.Consumption
import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.konfetti.core.failure.exponential
import com.davils.konfetti.core.failure.retry
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.core.limit.BusLimits
import com.davils.kore.core.validation.report.ValidationException
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlin.time.Duration.Companion.seconds

private const val TOO_LARGE_BUFFER: Int = 1_048_577
private const val TOO_MUCH_RETENTION: Int = 65_537
private const val TOO_MANY_ATTEMPTS: Int = 101

class EventBusLimitsTest : FunSpec({
    context("upper limits").config(coroutineTestScope = true) {
        test("refuse a buffer and a dead letter retention beyond what the bus carries") {
            val failure = shouldThrow<ValidationException> {
                bus {
                    deadLetterRetention = TOO_MUCH_RETENTION
                    topic(Topics.devices) { bufferCapacity = TOO_LARGE_BUFFER }
                }
            }

            failure.report.errors.map { violation -> violation.code } shouldContainExactly listOf(
                EventViolationCodes.INVALID_BUFFER,
                EventViolationCodes.INVALID_RETENTION,
            )
        }

        test("report a topic beyond the hard limits once, not again against the limits of the bus") {
            val failure = shouldThrow<ValidationException> {
                bus {
                    topic(Topics.devices) {
                        partitions = BusLimits.HARD_PARTITIONS + 1
                        bufferCapacity = BusLimits.HARD_BUFFER_CAPACITY + 1
                        retain = BusLimits.HARD_RETAIN + 1
                    }
                }
            }

            val codes = failure.report.errors.map { violation -> violation.code }
            codes.count { code -> code == EventViolationCodes.INVALID_PARTITIONS } shouldBe 1
            codes.count { code -> code == EventViolationCodes.INVALID_BUFFER } shouldBe 1
            codes.count { code -> code == EventViolationCodes.INVALID_RETENTION } shouldBe 1
        }

        test("refuse a mailbox, a retry and a growth factor beyond what a consumer can use") {
            val consumption = devices()[Topics.devices].consume(INDEXER) {
                mailboxCapacity = TOO_LARGE_BUFFER
                onFailure = retry(
                    attempts = TOO_MANY_ATTEMPTS,
                    backoff = exponential(initial = 1.seconds, max = 1.seconds, factor = Double.POSITIVE_INFINITY),
                )
                handle { }
            }

            val invalid = consumption.shouldBeInstanceOf<Consumption.Invalid>()
            invalid.report.errors.map { violation -> violation.code } shouldContainExactly listOf(
                EventViolationCodes.INVALID_MAILBOX,
                EventViolationCodes.INVALID_RETRY,
                EventViolationCodes.INVALID_BACKOFF,
            )
        }

        test("take defaults for every consumer from the bus") {
            val bus = devices { consumerDefaults { mailboxCapacity = TOO_LARGE_BUFFER } }

            val consumption = bus[Topics.devices].consume(INDEXER) { handle { } }

            val invalid = consumption.shouldBeInstanceOf<Consumption.Invalid>()
            invalid.report.errors.size shouldBe 1
        }
    }
})
