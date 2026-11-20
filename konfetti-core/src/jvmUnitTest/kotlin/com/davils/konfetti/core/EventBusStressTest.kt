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
import com.davils.konfetti.core.fixture.Reading
import com.davils.konfetti.core.fixture.Readings
import com.davils.konfetti.core.fixture.onThreads
import com.davils.konfetti.core.lifecycle.DrainResult
import com.davils.konfetti.core.observe.ConsumerMetrics
import io.kotest.assertions.withClue
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.minutes

private const val SENSORS: Int = 64
private const val READINGS: Int = 500
private const val PARTITIONS: Int = 8
private const val PUBLISHERS: Int = 8
private const val ROUNDS: Int = 5
private const val FAIL_EVERY: Int = 7

class EventBusStressTest : FunSpec({
    context("many publishers and consumers on many threads") {
        test("deliver every reading exactly once per consumer, in order per sensor") {
            onThreads { dispatcher ->
                repeat(ROUNDS) {
                    val scope = CoroutineScope(SupervisorJob() + dispatcher)
                    val bus = eventBus(scope) {
                        topic(Readings.topic) {
                            partitions = PARTITIONS
                            bufferCapacity = PARTITIONS
                            orderBy { reading -> reading.sensor }
                        }
                    }
                    val seen = ConcurrentHashMap<String, CopyOnWriteArrayList<Int>>()
                    val indexer = bus[Readings.topic].consume("telemetry.indexer") {
                        concurrency = PARTITIONS
                        mailboxCapacity = 2
                        handle { envelope ->
                            val reading = envelope.payload
                            seen.computeIfAbsent(reading.sensor) { CopyOnWriteArrayList() }.add(reading.sequence)
                            yield()
                        }
                    }
                    val flaky = bus[Readings.topic].consume("telemetry.flaky") {
                        concurrency = 2
                        onFailure = FailurePolicy.Skip
                        handle { envelope -> check(envelope.payload.sequence % FAIL_EVERY != 0) { "flaky" } }
                    }
                    val lossy = bus[Readings.topic].consume("telemetry.lossy") {
                        mailboxCapacity = 1
                        overflow = OverflowPolicy.DropOldest
                        handle { yield() }
                    }

                    coroutineScope {
                        repeat(PUBLISHERS) { publisher ->
                            launch(dispatcher) {
                                for (sequence in 0 until READINGS) {
                                    for (sensor in publisher until SENSORS step PUBLISHERS) {
                                        bus[Readings.topic].publish(Reading("sensor-$sensor", sequence))
                                    }
                                }
                            }
                        }
                    }

                    bus.drain(1.minutes) shouldBe DrainResult.Completed
                    scope.cancel()

                    val total = (SENSORS * READINGS).toLong()
                    seen.size shouldBe SENSORS
                    val clue = "indexer ${indexer.getOrThrow().metrics}, topic ${bus[Readings.topic].metrics}"
                    seen.forEach { (sensor, sequences) ->
                        withClue("$sensor missing ${(0 until READINGS) - sequences.toSet()} with $clue") {
                            sequences shouldContainExactly List(READINGS) { it }
                        }
                    }
                    indexer.getOrThrow().metrics.settledAll(total)
                    flaky.getOrThrow().metrics.settledAll(total)
                    lossy.getOrThrow().metrics.settledAll(total)
                    bus[Readings.topic].metrics.published shouldBe total
                    bus[Readings.topic].metrics.dispatched shouldBe total
                }
            }
        }
    }
})

private fun ConsumerMetrics.settledAll(total: Long) {
    received shouldBe total
    settled shouldBe total
    inFlight shouldBe 0
    queued shouldBe 0
}
