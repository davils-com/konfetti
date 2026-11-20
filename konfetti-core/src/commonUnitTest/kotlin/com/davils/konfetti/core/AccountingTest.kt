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

package com.davils.konfetti.core

import com.davils.konfetti.core.consume.OverflowPolicy
import com.davils.konfetti.core.failure.FailurePolicy
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.lifecycle.DrainResult
import io.kotest.common.ExperimentalKotest
import io.kotest.core.spec.style.FunSpec
import io.kotest.engine.coroutines.testScheduler
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.enum
import io.kotest.property.arbitrary.int
import io.kotest.property.checkAll
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.yield
import kotlin.time.Duration.Companion.hours

private val accounted: TopicKey<DeviceEvent> = TopicKey("inventory.accounted")

@OptIn(ExperimentalCoroutinesApi::class, ExperimentalKotest::class)
class AccountingTest : FunSpec({
    context("every accepted envelope").config(coroutineTestScope = true) {
        test("is settled exactly once by every consumer, whatever the policies") {
            checkAll(
                PropTestConfig(iterations = 40),
                Arb.int(1..4),
                Arb.int(1..4),
                Arb.enum<OverflowPolicy>(),
                Arb.int(1..60),
                Arb.boolean(),
            ) { partitions, mailbox, overflow, publications, closeEarly ->
                val bus = bus {
                    topic(accounted) {
                        this.partitions = partitions
                        bufferCapacity = 2
                        orderBy { event -> event.id }
                    }
                }
                val topic = bus[accounted]
                val consumers = listOf("inventory.fast", "inventory.flaky").map { name ->
                    topic.consume(name) {
                        concurrency = partitions
                        mailboxCapacity = mailbox
                        this.overflow = overflow
                        onFailure = FailurePolicy.Skip
                        handle { envelope ->
                            yield()
                            if (name.endsWith("flaky") && envelope.payload.id.length % 2 == 0) error("flaky")
                        }
                    }.getOrThrow()
                }

                repeat(publications) { index -> topic.tryPublish(DeviceEvent.Registered("d$index")) }
                if (closeEarly) bus.close()
                testScheduler.advanceTimeBy(1.hours)
                val result = bus.drain(1.hours)
                testScheduler.runCurrent()

                val metrics = topic.metrics
                (result == DrainResult.Completed || result == DrainResult.AlreadyClosed) shouldBe true
                metrics.published shouldBe metrics.dispatched + metrics.dropped
                consumers.forEach { consumer ->
                    val counts = consumer.metrics
                    counts.received shouldBe counts.settled
                    counts.inFlight shouldBe 0
                    counts.received shouldBe metrics.dispatched
                }
            }
        }
    }
})
