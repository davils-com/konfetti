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

package com.davils.konfetti.core.internal.delivery

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.consume.ConsumerConfiguration
import com.davils.konfetti.core.consume.ConsumerName
import com.davils.konfetti.core.consume.ConsumerState
import com.davils.konfetti.core.consume.StartPosition
import com.davils.konfetti.core.internal.bus.BusEnvironment
import com.davils.konfetti.core.internal.bus.DeadLetterSink
import com.davils.konfetti.core.lifecycle.DrainResult
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.ConsumerMetrics
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.core.topic.TopicName
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration

internal class ConsumerRuntime<T : Event>(
    configuration: ConsumerConfiguration<T>,
    val topic: TopicName,
    private val environment: BusEnvironment,
    deadLetters: DeadLetterSink,
    parent: CoroutineScope,
    private val release: (ConsumerRuntime<T>) -> Unit,
) {
    val name: ConsumerName = configuration.consumerName

    val start: StartPosition = configuration.intake.start

    private val counters = ConsumerCounters()
    private val scope = CoroutineScope(
        parent.coroutineContext +
            SupervisorJob(
                parent.coroutineContext[Job],
            ) +
            CoroutineName("konfetti-consumer($name)"),
    )
    private val lifecycle = ConsumerLifecycle(
        topic = topic,
        name = name,
        observers = environment.observers,
        scope = scope,
    )
    private val delivery = Delivery(
        consumer = name,
        configuration = configuration,
        environment = environment,
        counters = counters,
        deadLetters = deadLetters,
        lost = ::lost,
    )

    val workers: ConsumerWorkers<T> = ConsumerWorkers(
        intake = configuration.intake,
        scope = scope,
        counters = counters,
        lost = ::lost,
        reason = lifecycle::reason,
        deliver = ::deliver,
    )

    val state: StateFlow<ConsumerState> get() = lifecycle.state

    fun metrics(): ConsumerMetrics = counters.metrics()

    fun start() {
        workers.start()
        environment.observers.observe { BusObservation.ConsumerAttached(topic, name) }
    }

    fun watch(owner: CoroutineScope?) {
        lifecycle.watch(owner, ::detach)
    }

    fun detach() {
        lifecycle.detach()
        shutdown()
    }

    fun closeWith(reason: DropReason) {
        lifecycle.abort(reason)
        shutdown()
    }

    suspend fun drain(timeout: Duration): DrainResult {
        lifecycle.beginDrain()
        if (!lifecycle.isDraining()) return DrainResult.AlreadyClosed
        release(this)
        val before = counters.droppedCount
        val settled = withTimeoutOrNull(timeout) { drainFully() }
        return concluded(settled, before)
    }

    suspend fun drainFully() {
        lifecycle.beginDrain()
        workers.close()
        workers.join()
        detach()
    }

    private suspend fun concluded(settled: Unit?, before: Long): DrainResult {
        if (settled != null) return DrainResult.Completed
        closeWith(DropReason.Abandoned)
        workers.join()
        return DrainResult.TimedOut(counters.droppedCount - before)
    }

    private suspend fun deliver(envelope: Envelope<T>) {
        val outcome = delivery.deliver(envelope, lifecycle::reason)
        if (outcome !is DeliveryOutcome.Stop) return
        lifecycle.stop(outcome.cause)
        shutdown()
    }

    private fun shutdown() {
        release(this)
        workers.cancel()
    }

    private fun lost(envelope: Envelope<T>, reason: DropReason) {
        counters.countDropped()
        environment.recordLoss(reason)
        environment.observers.observe { BusObservation.Dropped(envelope, name, reason) }
    }
}
