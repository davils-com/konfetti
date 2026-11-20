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

import com.davils.konfetti.core.consume.Consumption
import com.davils.konfetti.core.fixture.Reading
import com.davils.konfetti.core.fixture.Readings
import com.davils.konfetti.core.fixture.Tally
import com.davils.konfetti.core.fixture.onThreads
import com.davils.konfetti.core.publish.PublishResult
import com.davils.konfetti.core.topic.Declaration
import io.kotest.assertions.withClue
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield
import java.util.concurrent.atomic.LongAdder
import kotlin.time.Duration.Companion.minutes

private const val ROUNDS: Int = 50
private const val PUBLICATIONS: Int = 2_000
private const val DECLARERS: Int = 16
private const val ATTACHERS: Int = 16
private const val INDEXER: String = "telemetry.indexer"
private const val PUBLISHERS: Int = 4

class EventBusRaceTest : FunSpec({
    context("closing while publishers and consumers are busy") {
        test("accounts for every accepted envelope as delivered or dropped") {
            onThreads { dispatcher ->
                repeat(ROUNDS) {
                    val scope = CoroutineScope(SupervisorJob() + dispatcher)
                    val tally = Tally()
                    val bus = eventBus(scope) {
                        observe(tally)
                        topic(Readings.topic) { bufferCapacity = 2 }
                    }
                    val consumption = bus[Readings.topic].consume(INDEXER) {
                        mailboxCapacity = 2
                        handle { yield() }
                    }
                    val consumer = consumption.getOrThrow()
                    val accepted = LongAdder()

                    coroutineScope {
                        launch(dispatcher) {
                            repeat(PUBLICATIONS) { sequence ->
                                val result = bus[Readings.topic].publish(Reading("sensor", sequence))
                                if (result is PublishResult.Published) accepted.increment()
                            }
                        }
                        launch(dispatcher) {
                            yield()
                            bus.close()
                        }
                    }
                    scope.cancel()

                    val topic = bus[Readings.topic].metrics
                    val metrics = consumer.metrics
                    withClue("published observations") { tally.published.sum() shouldBe accepted.sum() }
                    withClue("published count") { topic.published shouldBe accepted.sum() }
                    withClue("topic identity $topic") { topic.dispatched + topic.dropped shouldBe topic.published }
                    withClue("consumer identity $metrics") { metrics.settled shouldBe metrics.received }
                    withClue("received $metrics vs $topic") { metrics.received shouldBe topic.dispatched }
                    withClue("observed settlements") {
                        tally.settled.sum() + tally.dropped.sum() shouldBe topic.published
                    }
                }
            }
        }

        test("settles every envelope a consumer received, whether it was delivered or dropped") {
            onThreads { dispatcher ->
                repeat(ROUNDS) {
                    val scope = CoroutineScope(SupervisorJob() + dispatcher)
                    val bus = eventBus(scope) { topic(Readings.topic) }
                    val consumption = bus[Readings.topic].consume(INDEXER) { handle { yield() } }
                    val consumer = consumption.getOrThrow()

                    coroutineScope {
                        launch(dispatcher) {
                            repeat(
                                PUBLICATIONS,
                            ) { sequence -> bus[Readings.topic].publish(Reading("sensor", sequence)) }
                        }
                        launch(dispatcher) {
                            yield()
                            bus.drain(1.minutes)
                        }
                    }
                    bus.close()
                    scope.cancel()

                    val metrics = consumer.metrics
                    metrics.settled shouldBe metrics.received
                    metrics.inFlight shouldBe 0
                }
            }
        }
    }

    context("publishers that give up") {
        test("keep the account of the topic exact when they are cancelled while they wait") {
            onThreads { dispatcher ->
                repeat(ROUNDS) {
                    val scope = CoroutineScope(SupervisorJob() + dispatcher)
                    val bus = eventBus(scope) { topic(Readings.topic) { bufferCapacity = 1 } }
                    val consumption = bus[Readings.topic].consume(INDEXER) {
                        mailboxCapacity = 1
                        handle { yield() }
                    }
                    val consumer = consumption.getOrThrow()
                    val accepted = LongAdder()

                    coroutineScope {
                        val publishers = List(PUBLISHERS) { publisher ->
                            launch(dispatcher) {
                                repeat(PUBLICATIONS / PUBLISHERS) { sequence ->
                                    val reading = Reading("sensor-$publisher", sequence)
                                    val result = bus[Readings.topic].publish(reading)
                                    if (result is PublishResult.Published) accepted.increment()
                                }
                            }
                        }
                        launch(dispatcher) {
                            yield()
                            publishers.forEach { publisher -> publisher.cancel() }
                        }
                    }
                    bus.drain(1.minutes)
                    scope.cancel()

                    val topic = bus[Readings.topic].metrics
                    val metrics = consumer.metrics
                    val unanswered = topic.published - accepted.sum()
                    withClue("accepted $topic") { (unanswered in 0..PUBLISHERS.toLong()) shouldBe true }
                    withClue("topic identity $topic") { topic.dispatched + topic.dropped shouldBe topic.published }
                    withClue("consumer identity $metrics") { metrics.settled shouldBe metrics.received }
                    withClue("received $metrics vs $topic") { metrics.received shouldBe topic.dispatched }
                }
            }
        }
    }

    context("declaring and attaching side by side") {
        test("declares a topic exactly once however many try at the same time") {
            onThreads { dispatcher ->
                val scope = CoroutineScope(SupervisorJob() + dispatcher)
                val bus = eventBus(scope)

                val declarations = coroutineScope {
                    List(DECLARERS) { async(dispatcher) { bus.declare(Readings.topic) } }.awaitAll()
                }

                declarations.count { declaration -> declaration is Declaration.Declared } shouldBe 1
                bus.close()
                scope.cancel()
            }
        }

        test("attaches a consumer name exactly once however many try at the same time") {
            onThreads { dispatcher ->
                val scope = CoroutineScope(SupervisorJob() + dispatcher)
                val bus = eventBus(scope) { topic(Readings.topic) }

                val consumptions = coroutineScope {
                    List(ATTACHERS) {
                        async(dispatcher) { bus[Readings.topic].consume(INDEXER) { handle { } } }
                    }.awaitAll()
                }

                consumptions.count { consumption -> consumption is Consumption.Attached } shouldBe 1
                bus.close()
                scope.cancel()
            }
        }
    }
})
