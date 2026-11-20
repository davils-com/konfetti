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

import com.davils.konfetti.core.bus.EventBusConfiguration
import com.davils.konfetti.core.error.TopicNotDeclaredException
import com.davils.konfetti.core.error.TopicTypeMismatchException
import com.davils.konfetti.core.failure.DeadLetter
import com.davils.konfetti.core.internal.bus.BusEnvironment
import com.davils.konfetti.core.internal.bus.BusLifecycle
import com.davils.konfetti.core.internal.bus.TopicDirectory
import com.davils.konfetti.core.lifecycle.BusState
import com.davils.konfetti.core.lifecycle.CloseReport
import com.davils.konfetti.core.lifecycle.DrainResult
import com.davils.konfetti.core.limit.BusLimits
import com.davils.konfetti.core.observe.BusSnapshot
import com.davils.konfetti.core.topic.Declaration
import com.davils.konfetti.core.topic.TopicBuilder
import com.davils.konfetti.core.topic.TopicConfiguration
import com.davils.konfetti.core.topic.TopicName
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.StateFlow
import kotlin.time.Duration

/**
 * An in-process event bus: typed topics, their consumers and the dead letters of the bus.
 *
 * A bus is made with [eventBus] on a scope of the caller and runs on its dispatcher; it starts no thread of its own.
 * It lives until it is drained with [drain], closed with [close] or [abort], or its scope ends. Every accepted
 * envelope is accounted for exactly once by every consumer that received it, and every envelope that is lost on the
 * way is counted and observed with its reason; nothing disappears silently. The bus holds envelopes in memory only:
 * what is still on its way when the process ends is lost.
 *
 * A bus is safe to use from any number of coroutines and threads.
 *
 * @since 1.0.0
 */
public class EventBus internal constructor(configuration: EventBusConfiguration, scope: CoroutineScope) {
    private val environment = BusEnvironment(
        sources = configuration.sources,
        hooks = configuration.hooks,
        defaults = configuration.defaults,
        limits = configuration.limits,
        deadLetterStall = configuration.deadLetterStall,
    )
    private val parent = CoroutineScope(
        scope.coroutineContext + SupervisorJob(scope.coroutineContext[Job]) + CoroutineName("konfetti-event-bus"),
    )
    private val directory = TopicDirectory(environment, parent, configuration.deadLetterRetention)
    private val lifecycle = BusLifecycle(environment, parent, directory)

    init {
        configuration.topics.values.forEach { topic -> directory.register(topic.typed()) }
        lifecycle.watch(scope)
    }

    /**
     * The state of the bus.
     *
     * @since 1.0.0
     */
    public val state: StateFlow<BusState> get() = lifecycle.state

    /**
     * The limits of the bus.
     *
     * @since 1.0.0
     */
    public val limits: BusLimits get() = environment.limits

    /**
     * The dead letters of the bus, the topic [TopicName.DEAD_LETTERS].
     *
     * @since 1.0.0
     */
    public val deadLetters: Topic<DeadLetter> get() = directory.deadLetters

    /**
     * The names of the declared topics, without the dead letters.
     *
     * @since 1.0.0
     */
    public val topicNames: Set<TopicName> get() = directory.names

    /**
     * Resolves the topic of [key].
     *
     * @param T the type of the events of the topic.
     * @param key the key of the topic.
     * @return the topic.
     * @throws TopicNotDeclaredException if the bus declares no topic of that name.
     * @throws TopicTypeMismatchException if the topic carries another event type than [key].
     * @since 1.0.0
     */
    @Suppress("UNCHECKED_CAST")
    public operator fun <T : Event> get(key: TopicKey<T>): Topic<T> {
        val topic = directory.find(key.name) ?: throw TopicNotDeclaredException(key.name, directory.names)
        if (topic.eventType != key.eventType) {
            throw TopicTypeMismatchException(key.name, topic.eventType, key.eventType)
        }
        return topic as Topic<T>
    }

    /**
     * Tells whether the bus declares a topic with the name of [key], whatever its type.
     *
     * @param key the key of the topic.
     * @return true if a topic of that name is declared.
     * @since 1.0.0
     */
    public operator fun contains(key: TopicKey<*>): Boolean = directory.find(key.name) != null

    /**
     * Declares the topic of [key] while the bus runs.
     *
     * @param T the type of the events of the topic.
     * @param key the key of the topic.
     * @param block sets the settings of the topic; they start from the topic defaults of the bus.
     * @return the declared topic, or why none was declared.
     * @since 1.0.0
     */
    public fun <T : Event> declare(key: TopicKey<T>, block: TopicBuilder<T>.() -> Unit = {}): Declaration<T> {
        val builder = TopicBuilder(key, environment.topicDefaults, environment.limits)
        builder.block()
        val report = builder.validate()
        if (!report.isValid) return Declaration.Invalid(report)
        val configuration = builder.build()
        return lifecycle.ifRunning { directory.claim(configuration, report) } ?: Declaration.Closed(key.name)
    }

    /**
     * Returns the state and the counts of the bus, its topics and their consumers now.
     *
     * @return the snapshot.
     * @since 1.0.0
     */
    public fun snapshot(): BusSnapshot = BusSnapshot(
        state = lifecycle.state.value,
        topics = directory.runtimes.map { runtime -> runtime.snapshot() },
        observerFailures = environment.observers.failureCount,
        interceptorFailures = environment.interceptorFailureCount,
    )

    /**
     * Drains the bus: it takes nothing new, hands every accepted envelope to its consumers topic by topic in the order
     * they were declared and the dead letters last, waits until every consumer settled them, and closes.
     *
     * A second call waits for the first and returns its result.
     *
     * @param timeout how long to wait; what is still unsettled then is dropped as abandoned and the bus closes.
     * @return [DrainResult.Completed], [DrainResult.TimedOut] with the abandoned envelopes, or
     * [DrainResult.AlreadyClosed] if the bus was closed before or during the drain.
     * @since 1.0.0
     */
    public suspend fun drain(timeout: Duration): DrainResult = lifecycle.drain(timeout)

    /**
     * Closes the bus at once and waits until every coroutine of the bus has ended: what is still on its way is dropped
     * as closed, and every attempt in progress is cancelled.
     *
     * @return what the close dropped; nothing if the bus was closed already.
     * @since 1.0.0
     */
    public suspend fun close(): CloseReport = lifecycle.close()

    /**
     * Closes the bus at once without waiting, such as from code that cannot suspend; what is still on its way is
     * dropped as closed, and the coroutines of the bus end on their own.
     *
     * @return true if this call closed the bus, false if it was closed already.
     * @since 1.0.0
     */
    public fun abort(): Boolean = lifecycle.closeNow()

    /**
     * Shows the bus with its state and the number of its topics.
     *
     * @return the bus as text.
     * @since 1.0.0
     */
    override fun toString(): String = "EventBus(state=${lifecycle.state.value}, topics=${directory.count})"
}

@Suppress("UNCHECKED_CAST")
private fun TopicConfiguration<*>.typed(): TopicConfiguration<Event> = this as TopicConfiguration<Event>
