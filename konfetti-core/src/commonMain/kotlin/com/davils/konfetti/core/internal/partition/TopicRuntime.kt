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

package com.davils.konfetti.core.internal.partition

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.consume.ConsumerName
import com.davils.konfetti.core.consume.Consumption
import com.davils.konfetti.core.internal.bus.BusEnvironment
import com.davils.konfetti.core.internal.bus.DeadLetterSink
import com.davils.konfetti.core.internal.delivery.ConsumerRuntime
import com.davils.konfetti.core.lifecycle.BusState
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.ConsumerMetrics
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.core.observe.TopicMetrics
import com.davils.konfetti.core.observe.TopicSnapshot
import com.davils.konfetti.core.topic.OrderingKey
import com.davils.konfetti.core.topic.TopicConfiguration
import com.davils.konfetti.core.topic.TopicName
import com.davils.kore.core.annotation.DavilsInternal
import com.davils.kore.core.collection.immutableMap
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.fetchAndIncrement

@OptIn(ExperimentalAtomicApi::class, DavilsInternal::class)
internal class TopicRuntime<T : Event>(
    val configuration: TopicConfiguration<T>,
    val environment: BusEnvironment,
    val deadLetters: DeadLetterSink,
    parent: CoroutineScope,
) {
    val name: TopicName get() = configuration.name

    val isDeadLetterTopic: Boolean get() = configuration.name == TopicName.DEAD_LETTERS

    val scope: CoroutineScope = CoroutineScope(
        parent.coroutineContext + SupervisorJob(parent.coroutineContext[Job]) + CoroutineName("konfetti-topic($name)"),
    )

    val counters: TopicCounters = TopicCounters()

    val registry: ConsumerRegistry<T> = ConsumerRegistry(
        topic = configuration.name,
        retain = configuration.retain,
        limit = environment.limits.maximumConsumers,
    )

    private val lifecycle = AtomicReference(BusState.Running)
    private val aborting = AtomicReference(DropReason.Closed)
    private val rotation = AtomicInt(0)
    private val settled = CompletableDeferred<Unit>()
    private val partitions = List(configuration.partitions) {
        Partition<T>(configuration.bufferCapacity, ::lostBeforeDispatch)
    }
    private val pumps = partitions.mapIndexed { index, partition ->
        partition.start(scope) { envelope -> dispatch(index, envelope) }
    }

    val state: BusState get() = lifecycle.load()

    val isRunning: Boolean get() = lifecycle.load() == BusState.Running

    val hasConsumers: Boolean get() = isRunning && (!registry.isEmpty || configuration.retain > 0)

    fun partitionFor(envelope: Envelope<T>): Partition<T> = partitions[indexFor(envelope.orderingKey)]

    fun attach(consumer: ConsumerRuntime<T>): Consumption? = registry.attach(consumer) { isRunning }

    fun lostBeforeDispatch(envelope: Envelope<T>) {
        counters.countDropped()
        val reason = aborting.load()
        environment.recordLoss(reason)
        environment.observers.observe { BusObservation.Dropped(envelope, consumer = null, reason = reason) }
    }

    suspend fun drain() {
        if (!lifecycle.compareAndSet(expectedValue = BusState.Running, newValue = BusState.Draining)) {
            settled.await()
            return
        }
        partitions.forEach { partition -> partition.close() }
        pumps.joinAll()
        coroutineScope {
            registry.consumers.forEach { consumer -> launch { consumer.drainFully() } }
        }
        lifecycle.store(BusState.Closed)
        scope.cancel()
        settled.complete(Unit)
    }

    fun abort(reason: DropReason) {
        if (lifecycle.exchange(BusState.Closed) == BusState.Closed) return
        aborting.store(reason)
        partitions.forEach { partition -> partition.cancel() }
        registry.consumers.forEach { consumer -> consumer.closeWith(reason) }
        scope.cancel()
        settled.complete(Unit)
    }

    fun metrics(): TopicMetrics = counters.metrics(consumers = registry.consumers.size, retained = registry.retained)

    fun snapshot(): TopicSnapshot {
        val consumers = registry.consumers.sortedBy { consumer -> consumer.name }
        val metrics: Map<ConsumerName, ConsumerMetrics> =
            consumers.associate { consumer -> consumer.name to consumer.metrics() }.immutableMap()
        return TopicSnapshot(
            name = name,
            state = state,
            partitions = partitions.size,
            metrics = metrics(),
            consumers = metrics,
        )
    }

    private fun indexFor(key: OrderingKey?): Int {
        if (key == null) return rotation.fetchAndIncrement().mod(partitions.size)
        return partitionOf(key, partitions.size, environment.partitionKey)
    }

    private suspend fun dispatch(partition: Int, envelope: Envelope<T>) {
        val targets = registry.record(partition, envelope) { lifecycle.load() != BusState.Closed }
        if (targets == null) {
            lostBeforeDispatch(envelope)
            return
        }
        fanOut(partition, envelope, targets)
    }

    private suspend fun fanOut(partition: Int, envelope: Envelope<T>, targets: List<ConsumerRuntime<T>>) {
        var reached = 0
        try {
            targets.forEach { consumer ->
                reached += 1
                consumer.workers.offer(partition, envelope)
            }
        } finally {
            targets.drop(reached).forEach { consumer -> consumer.workers.miss(envelope) }
            counters.countDispatched()
        }
    }
}
