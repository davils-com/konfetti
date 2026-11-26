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

@file:OptIn(DavilsInternal::class)

package com.davils.konfetti.core.internal.partition

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.consume.Consumption
import com.davils.konfetti.core.consume.StartPosition
import com.davils.konfetti.core.internal.delivery.ConsumerRuntime
import com.davils.konfetti.core.topic.TopicName
import com.davils.kore.core.annotation.DavilsInternal
import com.davils.kore.core.concurrency.guard
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
internal class ConsumerRegistry<T : Event>(private val topic: TopicName, retain: Int, private val limit: Int) {
    private val lock = guard()
    private val attached = AtomicReference<List<ConsumerRuntime<T>>>(emptyList())
    private val retention = Retention<T>(retain)

    val consumers: List<ConsumerRuntime<T>> get() = attached.load()

    val isEmpty: Boolean get() = attached.load().isEmpty()

    val retained: Int get() = lock.locked { retention.size }

    fun attach(consumer: ConsumerRuntime<T>, running: () -> Boolean): Consumption? = lock.locked {
        refusalFor(consumer, running()) ?: admit(consumer)
    }

    fun release(consumer: ConsumerRuntime<T>) {
        lock.locked { attached.store(attached.load() - consumer) }
    }

    fun record(partition: Int, envelope: Envelope<T>, open: () -> Boolean): List<ConsumerRuntime<T>>? {
        if (retention.isDisabled) return attached.load().takeIf { open() }
        return lock.locked {
            val targets = attached.load().takeIf { open() }
            targets?.also { retention.add(partition, envelope) }
        }
    }

    private fun refusalFor(consumer: ConsumerRuntime<T>, running: Boolean): Consumption? {
        if (!running) return Consumption.Closed(topic)
        return occupancyOf(consumer)
    }

    private fun occupancyOf(consumer: ConsumerRuntime<T>): Consumption? {
        val present = attached.load()
        if (present.any { other -> other.name == consumer.name }) return Consumption.Duplicate(topic, consumer.name)
        return Consumption.LimitReached(topic, limit).takeIf { present.size >= limit }
    }

    private fun admit(consumer: ConsumerRuntime<T>): Consumption? {
        if (consumer.start == StartPosition.Retained) {
            retention.snapshot().forEach { retained -> consumer.workers.preload(retained.partition, retained.envelope) }
        }
        attached.store(attached.load() + consumer)
        return null
    }
}
