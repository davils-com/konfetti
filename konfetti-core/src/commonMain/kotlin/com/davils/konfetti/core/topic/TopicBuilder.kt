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

package com.davils.konfetti.core.topic

import com.davils.konfetti.core.Event
import com.davils.konfetti.core.TopicKey
import com.davils.konfetti.core.internal.partition.TopicSettings
import com.davils.konfetti.core.limit.BusLimits
import com.davils.kore.core.annotation.DavilsDsl
import com.davils.kore.core.dsl.ValidatingBuilder

/**
 * Builds the settings of one topic; every property starts from the topic defaults of the bus.
 *
 * @since 1.0.0
 */
@DavilsDsl
public class TopicBuilder<T : Event> internal constructor(
    private val key: TopicKey<T>,
    defaults: TopicSettings,
    private val limits: BusLimits,
) : ValidatingBuilder<TopicConfiguration<T>>() {
    private var ordering: ((T) -> String)? = null

    override val subject: String get() = "topic '${key.name}'"

    /**
     * The partitions of the topic, between 1 and [BusLimits.maximumPartitions].
     *
     * Envelopes of one partition reach every consumer in the order they were published; envelopes with equal ordering
     * keys share a partition. Without [orderBy], envelopes go to the partitions in turn and lose their order.
     *
     * @since 1.0.0
     */
    public var partitions: Int = defaults.partitions

    /**
     * The envelopes each partition buffers, between 1 and [BusLimits.maximumBufferCapacity].
     *
     * A publication waits while its partition is full; [com.davils.konfetti.core.Topic.tryPublish] is rejected instead.
     *
     * @since 1.0.0
     */
    public var bufferCapacity: Int = defaults.bufferCapacity

    /**
     * The last envelopes the topic keeps for consumers that start from
     * [com.davils.konfetti.core.consume.StartPosition.Retained], between 0 and [BusLimits.maximumRetain].
     *
     * @since 1.0.0
     */
    public var retain: Int = defaults.retain

    /**
     * Derives the ordering key of each event with [key].
     *
     * The function runs on the publishing coroutine for every publication and must be fast and free of side effects.
     * A function that throws, or returns an empty or too long key, rejects the publication with
     * [com.davils.konfetti.core.publish.Rejection.InvalidOrderingKey].
     *
     * @param key the function that returns the ordering key of an event.
     * @since 1.0.0
     */
    public fun orderBy(key: (T) -> String) {
        ordering = key
    }

    override fun assemble(): TopicConfiguration<T> = TopicConfiguration(
        name = key.name,
        eventType = key.eventType,
        partitions = partitions,
        bufferCapacity = bufferCapacity,
        retain = retain,
        orderBy = ordering,
        limits = limits,
    )
}
