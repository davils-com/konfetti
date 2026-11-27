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

import com.davils.konfetti.core.TopicKey
import com.davils.konfetti.core.consume.Consumption
import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.konfetti.core.failure.fixed
import com.davils.konfetti.core.failure.retry
import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.fixture.codes
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.core.header.HeaderKey
import com.davils.konfetti.core.header.headersOf
import com.davils.konfetti.core.limit.BusLimits
import com.davils.konfetti.core.limit.busLimits
import com.davils.konfetti.core.publish.PublishResult
import com.davils.konfetti.core.publish.Rejection
import com.davils.konfetti.core.topic.Declaration
import com.davils.kore.core.validation.report.ValidationException
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

private val trace = HeaderKey.text("trace-id")

class RuntimeLimitsTest : FunSpec({
    context("topics").config(coroutineTestScope = true) {
        test("are refused at run time beyond the limit of the bus") {
            val bus = devices { limits = busLimits { maximumTopics = 1 } }

            val declaration = bus.declare(Topics.audits)

            declaration shouldBe Declaration.LimitReached(Topics.audits.name, 1)
            declaration.toString() shouldContain "limit=1"
            shouldThrow<IllegalStateException> { declaration.getOrThrow() }.message shouldContain "limit of 1"
        }

        test("fail the builder beyond the limit of the bus") {
            val failure = shouldThrow<ValidationException> {
                bus {
                    limits = busLimits { maximumTopics = 1 }
                    topic(Topics.devices)
                    topic(Topics.audits)
                }
            }

            failure.report.errors.map { violation -> violation.code } shouldContainExactly
                listOf(EventViolationCodes.TOO_MANY_TOPICS)
        }

        test("are checked against tighter limits of the bus once, even when declared before the limits") {
            val failure = shouldThrow<ValidationException> {
                bus {
                    topic(Topics.devices) {
                        partitions = 4
                        bufferCapacity = 8
                        retain = 8
                        orderBy { event -> event.id }
                    }
                    deadLetterRetention = 0
                    limits = busLimits {
                        maximumPartitions = 2
                        maximumBufferCapacity = 4
                        maximumRetain = 4
                    }
                }
            }

            failure.report.errors.map { violation -> violation.code } shouldContainExactly listOf(
                EventViolationCodes.INVALID_PARTITIONS,
                EventViolationCodes.INVALID_BUFFER,
                EventViolationCodes.INVALID_RETENTION,
            )
        }

        test("report a value beyond the hard ceiling once, even on a bus with tighter limits") {
            val failure = shouldThrow<ValidationException> {
                bus {
                    topic(Topics.devices) {
                        partitions = BusLimits.HARD_PARTITIONS + 1
                        bufferCapacity = BusLimits.HARD_BUFFER_CAPACITY + 1
                        retain = BusLimits.HARD_RETAIN + 1
                        orderBy { event -> event.id }
                    }
                    deadLetterRetention = 0
                    limits = busLimits {
                        maximumPartitions = 2
                        maximumBufferCapacity = 4
                        maximumRetain = 4
                    }
                }
            }

            failure.report.errors.map { violation -> violation.code } shouldContainExactly listOf(
                EventViolationCodes.INVALID_PARTITIONS,
                EventViolationCodes.INVALID_BUFFER,
                EventViolationCodes.INVALID_RETENTION,
            )
        }

        test("refuse a dead-letter stall that is not positive") {
            val failure = shouldThrow<ValidationException> { bus { deadLetterStall = 0.seconds } }

            failure.report.errors.map { violation -> violation.code } shouldContainExactly
                listOf(EventViolationCodes.INVALID_STALL)
        }
    }

    context("consumers").config(coroutineTestScope = true) {
        test("are refused beyond the limit of a topic") {
            val topic = devices { limits = busLimits { maximumConsumers = 1 } }[Topics.devices]
            topic.consume(INDEXER) { handle { } }

            val second = topic.consume("inventory.second") { handle { } }

            second shouldBe Consumption.LimitReached(Topics.devices.name, 1)
            second.hashCode() shouldBe Consumption.LimitReached(Topics.devices.name, 1).hashCode()
            shouldThrow<IllegalStateException> { second.getOrThrow() }.message shouldContain "limit of 1"
        }

        test("may not wait longer, retry more often or back off longer than the bus allows") {
            val topic = devices {
                limits = busLimits {
                    maximumHandlingTimeout = 1.hours
                    maximumAttempts = 2
                    maximumRetryDelay = 1.hours
                }
            }[Topics.devices]

            val consumption = topic.consume(INDEXER) {
                handlingTimeout = 2.hours
                onFailure = retry(attempts = 3, backoff = fixed(2.hours))
                handle { }
            }

            consumption.codes shouldContainExactly
                listOf(
                    EventViolationCodes.INVALID_TIMEOUT,
                    EventViolationCodes.INVALID_RETRY,
                    EventViolationCodes.INVALID_BACKOFF,
                )
        }

        test("refuse a longest stall that is not positive") {
            val consumption = devices()[Topics.devices].consume(INDEXER) {
                maximumStall = 0.seconds
                handle { }
            }

            consumption.codes shouldContainExactly
                listOf(EventViolationCodes.INVALID_STALL)
        }
    }

    context("publications").config(coroutineTestScope = true) {
        test("are rejected with more headers than the bus allows") {
            val topic = devices { limits = busLimits { maximumHeaders = 1 } }[Topics.devices]
            val headers = headersOf(trace("t-1"), HeaderKey.text("tenant")("acme"))

            topic.publish(DeviceEvent.Registered("r1"), headers = headers) shouldBe
                PublishResult.Rejected(Topics.devices.name, Rejection.OversizedHeaders)
            topic.metrics.rejected shouldBe 1
        }

        test("are rejected with a header value longer than the bus allows, after the interceptors") {
            val topic = devices {
                limits = busLimits { maximumHeaderValueLength = 4 }
                enrich { publication -> publication.headers.with(trace("too-long")) }
            }[Topics.devices]

            topic.tryPublish(DeviceEvent.Registered("r1")) shouldBe
                PublishResult.Rejected(Topics.devices.name, Rejection.OversizedHeaders)
        }

        test("are rejected when the ordering key throws, is empty or is too long") {
            val keyed = TopicKey<AuditEvent>("audit.keyed")
            val topic = bus {
                limits = busLimits { maximumOrderingKeyLength = 4 }
                topic(keyed) {
                    partitions = 2
                    orderBy { event -> event.text.takeUnless { text -> text == "throw" } ?: error("no key") }
                }
            }[keyed]
            val rejected = PublishResult.Rejected(keyed.name, Rejection.InvalidOrderingKey)

            topic.publish(AuditEvent("throw")) shouldBe rejected
            topic.publish(AuditEvent("")) shouldBe rejected
            topic.offer(AuditEvent("longer")) shouldBe rejected
            topic.publish(AuditEvent("fits")).shouldBeInstanceOf<PublishResult.Published<AuditEvent>>()
        }
    }
})
