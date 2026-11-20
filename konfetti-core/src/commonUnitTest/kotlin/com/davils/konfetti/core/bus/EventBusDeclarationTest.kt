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

import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.konfetti.core.error.TopicNotDeclaredException
import com.davils.konfetti.core.error.TopicTypeMismatchException
import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.topic.Declaration
import com.davils.konfetti.core.topic.TopicName
import com.davils.konfetti.testing.RecordingObserver
import com.davils.kore.core.validation.report.ValidationException
import com.davils.kore.core.validation.violation.Severity
import com.davils.kore.core.validation.violation.ViolationCode
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf

private const val TOO_MANY_PARTITIONS: Int = 1_025
private const val TOO_MUCH_RETENTION: Int = 65_537
private const val PARTITIONS: Int = 4

class EventBusDeclarationTest : FunSpec({
    context("declaring in the builder").config(coroutineTestScope = true) {
        test("carries the topics it was given, in declaration order") {
            val bus = bus {
                topic(Topics.devices)
                topic(Topics.audits)
            }

            bus.topicNames.toList() shouldContainExactly listOf(Topics.devices.name, Topics.audits.name)
            (Topics.devices in bus) shouldBe true
            bus.toString() shouldBe "EventBus(state=Running, topics=2)"
        }

        test("reports every problem of every topic at once, each under its path") {
            val failure = shouldThrow<ValidationException> {
                bus {
                    topic(Topics.devices) {
                        partitions = 0
                        bufferCapacity = 0
                    }
                    topic(Topics.audits) { retain = -1 }
                }
            }

            val paths = failure.report.errors.map { violation -> violation.path.toString() to violation.code }
            paths shouldContainExactly listOf(
                "topics[\"inventory.devices\"].partitions" to EventViolationCodes.INVALID_PARTITIONS,
                "topics[\"inventory.devices\"].bufferCapacity" to EventViolationCodes.INVALID_BUFFER,
                "topics[\"audit.entries\"].retain" to EventViolationCodes.INVALID_RETENTION,
            )
        }

        test("refuses limits beyond what a topic can carry") {
            val failure = shouldThrow<ValidationException> {
                bus {
                    topic(Topics.devices) {
                        partitions = TOO_MANY_PARTITIONS
                        retain = TOO_MUCH_RETENTION
                    }
                }
            }

            failure.report.errors.map { violation -> violation.code } shouldContainExactly listOf(
                EventViolationCodes.INVALID_PARTITIONS,
                EventViolationCodes.INVALID_RETENTION,
            )
        }

        test("refuses the same topic twice") {
            val failure = shouldThrow<ValidationException> {
                bus {
                    topic(Topics.devices)
                    topic(Topics.devices)
                }
            }

            failure.report.errors
                .single()
                .code shouldBe ViolationCode.DUPLICATE
        }

        test("refuses the name of the dead letters") {
            val failure = shouldThrow<ValidationException> {
                bus { topic(Topics.reserved) }
            }

            failure.report.errors
                .single()
                .code shouldBe EventViolationCodes.RESERVED_NAME
        }

        test("refuses a negative dead letter retention") {
            val failure = shouldThrow<ValidationException> { bus { deadLetterRetention = -1 } }

            failure.report.errors
                .single()
                .code shouldBe EventViolationCodes.INVALID_RETENTION
        }

        test("applies topic defaults to the topics declared after them") {
            val bus = bus {
                topicDefaults { partitions = PARTITIONS }
                topic(Topics.devices) { orderBy { event -> event.id } }
            }

            bus[Topics.devices].partitionCount shouldBe PARTITIONS
        }

        test("starts a bus with nothing but its dead letters") {
            val bus = bus()

            bus.topicNames shouldBe emptySet()
            bus.deadLetters.name shouldBe TopicName("konfetti.dead-letters")
        }
    }

    context("warnings").config(coroutineTestScope = true) {
        test("warn about defaults changed after a topic and about partitions without ordering") {
            val builder = EventBusBuilder()
            builder.topic(Topics.devices) { partitions = PARTITIONS }
            builder.topicDefaults { retain = 1 }

            val report = builder.validate()

            report.isValid shouldBe true
            report.warnings.map { warning -> warning.code } shouldContainExactly listOf(
                EventViolationCodes.UNORDERED_PARTITIONS,
                EventViolationCodes.LATE_DEFAULT,
            )
            report.warnings.map { warning -> warning.severity }.distinct() shouldBe listOf(Severity.Warning)
        }
    }

    context("resolving").config(coroutineTestScope = true) {
        test("refuses a key whose topic is not declared") {
            val bus = devices()

            val failure = shouldThrow<TopicNotDeclaredException> { bus[Topics.audits] }

            failure.topic shouldBe Topics.audits.name
            failure.message shouldContain "['inventory.devices']"
        }

        test("names no topics when the bus carries none") {
            val failure = shouldThrow<TopicNotDeclaredException> { bus()[Topics.audits] }

            failure.message shouldContain "no topics"
        }

        test("refuses a key that names the topic with another type") {
            val bus = devices()

            val failure = shouldThrow<TopicTypeMismatchException> { bus[Topics.impostor] }

            failure.declared shouldBe DeviceEvent::class
            failure.requested shouldBe AuditEvent::class
            (Topics.audits in bus) shouldBe false
        }
    }

    context("declaring at runtime").config(coroutineTestScope = true) {
        test("adds a topic and reports it") {
            val observer = RecordingObserver()
            val bus = bus { observe(observer) }

            val declaration = bus.declare(Topics.audits)

            val declared = declaration.shouldBeInstanceOf<Declaration.Declared<AuditEvent>>()
            declared.report.isValid shouldBe true
            declaration.getOrThrow() shouldBe bus[Topics.audits]
            observer.only<BusObservation.TopicDeclared>().single().topic shouldBe Topics.audits.name
        }

        test("hands back the warnings of a valid declaration") {
            val declaration = bus().declare(Topics.devices) { partitions = PARTITIONS }

            val declared = declaration.shouldBeInstanceOf<Declaration.Declared<DeviceEvent>>()
            declared.report.warnings
                .single()
                .code shouldBe EventViolationCodes.UNORDERED_PARTITIONS
        }

        test("refuses a topic that is already declared") {
            val declaration = devices().declare(Topics.devices)

            declaration shouldBe Declaration.AlreadyDeclared(Topics.devices.name)
            shouldThrow<IllegalStateException> { declaration.getOrThrow() }
        }

        test("refuses an invalid topic with the full report") {
            val declaration = bus().declare(Topics.devices) { partitions = 0 }

            val invalid = declaration.shouldBeInstanceOf<Declaration.Invalid<DeviceEvent>>()
            invalid.report.errors
                .single()
                .code shouldBe EventViolationCodes.INVALID_PARTITIONS
            shouldThrow<IllegalArgumentException> { declaration.getOrThrow() }
        }

        test("refuses a topic once the bus is closed") {
            val bus = bus()
            bus.close()

            val declaration = bus.declare(Topics.audits)

            declaration shouldBe Declaration.Closed(Topics.audits.name)
            shouldThrow<IllegalStateException> { declaration.getOrThrow() }
        }
    }
})
