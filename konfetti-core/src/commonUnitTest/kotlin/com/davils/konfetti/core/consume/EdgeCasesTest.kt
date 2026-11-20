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

package com.davils.konfetti.core.consume

import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.konfetti.core.failure.exponential
import com.davils.konfetti.core.failure.retry
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.attach
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.core.fixture.send
import com.davils.konfetti.core.fixture.settle
import com.davils.konfetti.core.limit.BusLimits
import com.davils.konfetti.core.limit.busLimits
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.core.observe.ObservationStream
import com.davils.konfetti.core.topic.Declaration
import com.davils.konfetti.testing.RecordingObserver
import com.davils.kore.core.validation.report.ValidationException
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.awaitCancellation
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.seconds

class EdgeCasesTest : FunSpec({
    context("handling").config(coroutineTestScope = true) {
        test("finishes within its timeout") {
            val consumer = devices()[Topics.devices].let { topic ->
                topic.attach(INDEXER) {
                    handlingTimeout = 5.seconds
                    handle { }
                }.also { topic.send(DeviceEvent.Registered("r1")) }
            }
            settle()

            consumer.metrics.delivered shouldBe 1
        }

        test("drops what was in hand when the consumer is detached") {
            val observer = RecordingObserver()
            val topic = devices { observe(observer) }[Topics.devices]
            val consumer = topic.attach(INDEXER) { handle { awaitCancellation() } }
            topic.send(DeviceEvent.Registered("r1"))
            settle()

            consumer.detach()
            settle()

            consumer.metrics.dropped shouldBe 1
            consumer.metrics.inFlight shouldBe 0
            observer.only<com.davils.konfetti.core.observe.BusObservation.Dropped>().single().reason shouldBe
                DropReason.Detached
        }

        test("backs off no longer than the cap when the growth overflows") {
            val topic = devices()[Topics.devices]
            val consumer = topic.attach(INDEXER) {
                onFailure = retry(attempts = 3, backoff = exponential(1.seconds, 2.seconds, factor = Double.MAX_VALUE))
                handle { error("fails") }
            }

            topic.send(DeviceEvent.Registered("r1"))
            settle()

            consumer.metrics.retried shouldBe 2
            consumer.metrics.deadLettered shouldBe 1
        }
    }

    context("validation").config(coroutineTestScope = true) {
        test("refuses fewer than one attempt and reports an attempt count beyond the ceiling once") {
            val topic = devices()[Topics.devices]

            val none = topic.consume(INDEXER) {
                onFailure = retry(attempts = 0)
                handle { }
            }
            val tooMany = topic.consume(INDEXER) {
                onFailure = retry(attempts = BusLimits.HARD_ATTEMPTS + 1)
                handle { }
            }

            none.shouldBeInstanceOf<Consumption.Invalid>().report.errors.map { violation -> violation.code } shouldContainExactly
                listOf(EventViolationCodes.INVALID_RETRY)
            tooMany.shouldBeInstanceOf<Consumption.Invalid>().report.errors.size shouldBe 1
            none.toString() shouldContain "errors=1"
        }

        test("reports a topic beyond the hard ceilings once, whatever the limits of the bus") {
            val failure = shouldThrow<ValidationException> {
                bus {
                    deadLetterRetention = 0
                    limits = busLimits { maximumPartitions = 2 }
                    topic(Topics.devices) {
                        partitions = BusLimits.HARD_PARTITIONS + 1
                        bufferCapacity = BusLimits.HARD_BUFFER_CAPACITY + 1
                        retain = BusLimits.HARD_RETAIN + 1
                    }
                }
            }

            failure.report.errors.size shouldBe 3
        }

        test("refuses a handling timeout beyond the hard ceiling of the bus") {
            val consumption = devices()[Topics.devices].consume(INDEXER) {
                handlingTimeout = 30.days
                handle { }
            }

            consumption.shouldBeInstanceOf<Consumption.Invalid>()
        }

        test("refuses an observation stream beyond its largest capacity") {
            shouldThrow<IllegalArgumentException> { ObservationStream(capacity = ObservationStream.MAX_CAPACITY + 1) }
        }
    }

    context("snapshots and texts").config(coroutineTestScope = true) {
        test("sort the consumers of a topic by name") {
            val bus = devices()
            bus[Topics.devices].attach("zeta.reader") { handle { } }
            bus[Topics.devices].attach("alpha.reader") { handle { } }

            val consumers = bus.snapshot().topics.first().consumers.keys.map { name -> name.value }

            consumers shouldContainExactly listOf("alpha.reader", "zeta.reader")
            bus.limits shouldBe BusLimits.Default
        }

        test("show a declaration and an attached consumer") {
            val bus = devices()

            val declared = bus.declare(Topics.audits)
            val attached = bus[Topics.devices].consume(INDEXER) { handle { } }

            declared.toString() shouldBe "Declared(audit.entries)"
            declared.shouldBeInstanceOf<Declaration.Declared<*>>().getOrThrow().name shouldBe Topics.audits.name
            attached.toString() shouldBe "Attached(Consumer(inventory.devices/inventory.indexer))"
            bus.declare(Topics.reserved).toString() shouldContain "Invalid"
        }
    }
})
