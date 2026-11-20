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

package com.davils.konfetti.core.internal.bus

import com.davils.konfetti.core.Event
import com.davils.konfetti.core.Topic
import com.davils.konfetti.core.failure.DeadLetter
import com.davils.konfetti.core.internal.partition.TopicRuntime
import com.davils.konfetti.core.internal.platform.guard
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.topic.Declaration
import com.davils.konfetti.core.topic.TopicConfiguration
import com.davils.konfetti.core.topic.TopicName
import com.davils.kore.core.annotation.DavilsInternal
import com.davils.kore.core.collection.OrderedMap
import com.davils.kore.core.collection.immutableSet
import com.davils.kore.core.validation.report.ValidationReport
import kotlinx.coroutines.CoroutineScope
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

private const val DEAD_LETTER_BUFFER: Int = 64

@OptIn(ExperimentalAtomicApi::class, DavilsInternal::class)
internal class TopicDirectory(
    private val environment: BusEnvironment,
    private val parent: CoroutineScope,
    deadLetterRetention: Int,
) {
    private val lock = guard()
    private val byName = OrderedMap<TopicName, Topic<*>>()
    private val declared = AtomicReference<List<Topic<*>>>(emptyList())

    val deadLetters: Topic<DeadLetter> = Topic(
        TopicRuntime(
            configuration = TopicConfiguration(
                name = TopicName.DEAD_LETTERS,
                eventType = DeadLetter::class,
                partitions = 1,
                bufferCapacity = DEAD_LETTER_BUFFER,
                retain = deadLetterRetention,
                orderBy = null,
                limits = environment.limits,
            ),
            environment = environment,
            deadLetters = DeadLetterSink.Refusing,
            parent = parent,
        ),
    )

    private val sink = DeadLetterSink { letter, cause -> deadLetters.store(letter, cause, environment.deadLetterStall) }

    val names: Set<TopicName> get() = lock.locked { byName.keys.immutableSet() }

    val count: Int get() = declared.load().size

    val runtimes: List<TopicRuntime<*>>
        get() = declared.load().map { topic -> topic.runtime } + deadLetters.runtime

    fun find(name: TopicName): Topic<*>? = lock.locked { byName[name] }

    fun <T : Event> register(configuration: TopicConfiguration<T>): Topic<T> {
        val topic = Topic(
            TopicRuntime(configuration = configuration, environment = environment, deadLetters = sink, parent = parent),
        )
        lock.locked {
            byName[configuration.name] = topic
            declared.store(declared.load() + topic)
        }
        environment.observers.announce { BusObservation.TopicDeclared(configuration.name) }
        return topic
    }

    fun <T : Event> claim(configuration: TopicConfiguration<T>, report: ValidationReport): Declaration<T> {
        val refusal = lock.locked { refusalFor(configuration.name) }
        if (refusal != null) return refusal.typed()
        return Declaration.Declared(register(configuration), report)
    }

    private fun refusalFor(name: TopicName): Declaration<Event>? {
        if (name in byName) return Declaration.AlreadyDeclared(name)
        val limit = environment.limits.maximumTopics
        return Declaration.LimitReached<Event>(name, limit).takeIf { byName.size >= limit }
    }
}

@Suppress("UNCHECKED_CAST")
private fun <T : Event> Declaration<Event>.typed(): Declaration<T> = this as Declaration<T>
