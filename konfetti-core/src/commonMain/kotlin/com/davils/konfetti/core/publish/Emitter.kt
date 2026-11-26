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

package com.davils.konfetti.core.publish

import com.davils.konfetti.core.Event
import com.davils.konfetti.core.EventBus
import com.davils.konfetti.core.Topic
import com.davils.konfetti.core.TopicKey
import com.davils.konfetti.core.topic.TopicBuilder
import com.davils.kore.core.failure.contained
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/**
 * The telemetry side of a library that reports what it does as events on a topic of the application's bus, without
 * ever letting the bus fail or slow the work it reports on.
 *
 * An emitter resolves the topic of [key] once, when it is created: it declares the topic with [settings] if the bus
 * does not declare it yet, and takes the declared one otherwise. Without a bus, on a closed bus, or when the bus
 * declares the name for another event type, the emitter is unbound and drops every event; nothing of that reaches the
 * caller.
 *
 * [emit] builds an event only while the topic has consumers, so an event nobody listens to costs nothing, and offers it
 * without waiting. Building and offering are contained: an exception of the builder, a full partition or a closed bus
 * never reaches the caller, and every event that was built but not published is counted in [rejected]. An emitter is
 * safe to share between threads and coroutines.
 *
 * @param E the type of the events.
 * @param bus the bus of the application, or null when it has none.
 * @param key the key of the topic.
 * @param settings sets the topic when the emitter declares it; it starts from the topic defaults of the bus.
 * @since 1.1.0
 */
@OptIn(ExperimentalAtomicApi::class)
public class Emitter<E : Event>(
    bus: EventBus?,
    public val key: TopicKey<E>,
    settings: TopicBuilder<E>.() -> Unit = {},
) {
    private val topic: Topic<E>? = bus?.let { found -> contained { topicOf(found, settings) }.getOrNull() }
    private val rejectedCount: AtomicLong = AtomicLong(0L)

    /**
     * Whether the emitter found its topic; an unbound emitter drops every event.
     *
     * @since 1.1.0
     */
    public val isBound: Boolean get() = topic != null

    /**
     * Whether the topic has a consumer now, so that an event would be delivered.
     *
     * @since 1.1.0
     */
    public val hasConsumers: Boolean get() = topic?.hasConsumers == true

    /**
     * The number of events that were built but not published: refused by the bus or failed while they were offered.
     *
     * @since 1.1.0
     */
    public val rejected: Long get() = rejectedCount.load()

    /**
     * Builds an event with [event] and offers it to the topic, if the topic has consumers.
     *
     * @param event builds the event; it is not called while nobody listens.
     * @return true if the bus published the event.
     * @since 1.1.0
     */
    public fun emit(event: () -> E): Boolean {
        if (!hasConsumers) return false
        val built = contained(event)
        return built.fold(onSuccess = ::send, onFailure = { rejectedOnce() })
    }

    /**
     * Offers an event that is already built to the topic, whether or not it has consumers.
     *
     * @param event the event.
     * @return true if the bus published the event; false if the emitter is unbound or the bus refused it.
     * @since 1.1.0
     */
    public fun send(event: E): Boolean {
        val target = topic ?: return false
        val result = contained { target.offer(event) }.getOrNull()
        val isPublished = result is PublishResult.Published
        if (!isPublished) rejectedOnce()
        return isPublished
    }

    private fun rejectedOnce(): Boolean {
        rejectedCount.addAndFetch(1L)
        return false
    }

    /**
     * Shows the topic and whether the emitter is bound, never an event.
     *
     * @return the emitter as text.
     * @since 1.1.0
     */
    override fun toString(): String = "Emitter(topic=${key.name}, bound=$isBound)"

    private fun topicOf(bus: EventBus, settings: TopicBuilder<E>.() -> Unit): Topic<E> {
        if (key !in bus) bus.declare(key, settings)
        return bus[key]
    }
}
