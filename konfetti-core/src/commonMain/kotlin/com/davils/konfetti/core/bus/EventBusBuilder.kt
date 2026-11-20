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

package com.davils.konfetti.core.bus

import com.davils.konfetti.core.Event
import com.davils.konfetti.core.TopicKey
import com.davils.konfetti.core.consume.ConsumeInterceptor
import com.davils.konfetti.core.consume.ConsumerDefaults
import com.davils.konfetti.core.internal.bus.BusDefaults
import com.davils.konfetti.core.internal.bus.BusHooks
import com.davils.konfetti.core.internal.bus.BusSources
import com.davils.konfetti.core.internal.bus.SecureEntropy
import com.davils.konfetti.core.limit.BusLimits
import com.davils.konfetti.core.observe.BusObserver
import com.davils.konfetti.core.publish.PublishInterceptor
import com.davils.konfetti.core.topic.TopicBuilder
import com.davils.konfetti.core.topic.TopicConfiguration
import com.davils.konfetti.core.topic.TopicDefaults
import com.davils.kore.core.annotation.DavilsDsl
import com.davils.kore.core.dsl.ValidatingBuilder
import com.davils.kore.core.identity.UuidV7
import com.davils.kore.core.identity.generator.UuidGenerator
import com.davils.kore.core.identity.generator.UuidV7Generator
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

private const val DEFAULT_DEAD_LETTER_RETENTION: Int = 64
private val defaultDeadLetterStall: Duration = 10.seconds

/**
 * Builds an event bus: its topics, observers, interceptors, defaults, limits and sources.
 *
 * @since 1.0.0
 */
@DavilsDsl
public class EventBusBuilder internal constructor() : ValidatingBuilder<EventBusConfiguration>() {
    private val topics = linkedMapOf<String, TopicConfiguration<*>>()
    private val observers = mutableListOf<BusObserver>()
    private val publishInterceptors = mutableListOf<PublishInterceptor>()
    private val consumeInterceptors = mutableListOf<ConsumeInterceptor>()
    private val topicDefaults = TopicDefaults()
    private val consumerDefaults = ConsumerDefaults()
    private var hasLateTopicDefaults = false

    override val subject: String get() = "event bus"

    /**
     * The generator of the ids of envelopes: the monotonic UUID version 7 generator of Kore by default.
     *
     * @since 1.0.0
     */
    public var identifiers: UuidGenerator<UuidV7> = UuidV7Generator

    /**
     * The time source that measures how long attempts take: the monotonic time source by default.
     *
     * @since 1.0.0
     */
    public var timeSource: TimeSource.WithComparableMarks = TimeSource.Monotonic

    /**
     * The randomness of the bus; the secure randomness of the platform by default. See [Entropy].
     *
     * @since 1.0.0
     */
    public var entropy: Entropy = SecureEntropy

    /**
     * The limits of the bus: [BusLimits.Default] by default.
     *
     * @since 1.0.0
     */
    public var limits: BusLimits = BusLimits.Default

    /**
     * The last dead letters the bus retains for consumers that start from the retained envelopes, between 0 and
     * [BusLimits.maximumRetain]: 64 by default.
     *
     * @since 1.0.0
     */
    public var deadLetterRetention: Int = DEFAULT_DEAD_LETTER_RETENTION

    /**
     * How long a failing consumer waits at most for room in the dead letters, positive: 10 seconds by default.
     *
     * An envelope whose dead letter finds no room in time is dropped with
     * [com.davils.konfetti.core.observe.DropReason.DeadLetterOverflow], so a slow consumer of the dead letters never
     * stops the consumers that fail.
     *
     * @since 1.0.0
     */
    public var deadLetterStall: Duration = defaultDeadLetterStall

    /**
     * Adds an observer of the bus.
     *
     * @param observer the observer.
     * @since 1.0.0
     */
    public fun observe(observer: BusObserver) {
        observers += observer
    }

    /**
     * Adds an interceptor that enriches every publication of the bus.
     *
     * @param interceptor the interceptor.
     * @since 1.0.0
     */
    public fun enrich(interceptor: PublishInterceptor) {
        publishInterceptors += interceptor
    }

    /**
     * Adds an interceptor that wraps every attempt of every consumer of the bus.
     *
     * @param interceptor the interceptor.
     * @since 1.0.0
     */
    public fun intercept(interceptor: ConsumeInterceptor) {
        consumeInterceptors += interceptor
    }

    /**
     * Changes the settings every topic declared after this call starts from.
     *
     * @param block sets the defaults.
     * @since 1.0.0
     */
    public fun topicDefaults(block: TopicDefaults.() -> Unit) {
        topicDefaults.block()
        if (topics.isNotEmpty()) hasLateTopicDefaults = true
    }

    /**
     * Changes the settings every consumer of the bus starts from.
     *
     * @param block sets the defaults.
     * @since 1.0.0
     */
    public fun consumerDefaults(block: ConsumerDefaults.() -> Unit) {
        consumerDefaults.block()
    }

    /**
     * Declares the topic of [key]; a topic declared twice is reported as a duplicate.
     *
     * @param T the type of the events of the topic.
     * @param key the key of the topic.
     * @param block sets the settings of the topic.
     * @since 1.0.0
     */
    public fun <T : Event> topic(key: TopicKey<T>, block: TopicBuilder<T>.() -> Unit = {}) {
        val builder = TopicBuilder(key, topicDefaults.snapshot(), BusLimits.Ceiling)
        collect(
            target = topics,
            key = key.name.value,
            builder = builder,
            property = EventBusConfiguration::topics,
            block = block,
        )
    }

    override fun assemble(): EventBusConfiguration = EventBusConfiguration(
        topics = topics.toMap(),
        deadLetterRetention = deadLetterRetention,
        deadLetterStall = deadLetterStall,
        limits = limits,
        hooks = BusHooks(
            observers = observers.toList(),
            publishInterceptors = publishInterceptors.toList(),
            consumeInterceptors = consumeInterceptors.toList(),
        ),
        sources = BusSources(identifiers = identifiers, timeSource = timeSource, entropy = entropy),
        defaults = BusDefaults(topic = topicDefaults.snapshot(), consumer = consumerDefaults.snapshot()),
        hasLateTopicDefaults = hasLateTopicDefaults,
    )
}
