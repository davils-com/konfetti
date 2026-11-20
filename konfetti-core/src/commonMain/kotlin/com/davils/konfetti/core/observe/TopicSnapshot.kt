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

package com.davils.konfetti.core.observe

import com.davils.konfetti.core.consume.ConsumerName
import com.davils.konfetti.core.lifecycle.BusState
import com.davils.konfetti.core.topic.TopicName

/**
 * The state and counts of one topic and its consumers at one moment.
 *
 * @property name the name of the topic.
 * @property state the state of the topic.
 * @property partitions the partitions of the topic.
 * @property metrics the counts of the topic.
 * @property consumers the counts of every attached consumer, sorted by name.
 * @since 1.0.0
 */
public class TopicSnapshot internal constructor(
    public val name: TopicName,
    public val state: BusState,
    public val partitions: Int,
    public val metrics: TopicMetrics,
    public val consumers: Map<ConsumerName, ConsumerMetrics>,
) {
    private val fields: List<Any> get() = listOf(name, state, partitions, metrics, consumers)

    /**
     * Tells whether [other] is an equal snapshot.
     *
     * @param other the object to compare with.
     * @return true if [other] holds the same state and counts.
     * @since 1.0.0
     */
    override fun equals(other: Any?): Boolean = other is TopicSnapshot && fields == other.fields

    /**
     * Returns a hash consistent with [equals].
     *
     * @return the hash.
     * @since 1.0.0
     */
    override fun hashCode(): Int = fields.hashCode()

    /**
     * Shows the snapshot.
     *
     * @return the snapshot as text.
     * @since 1.0.0
     */
    override fun toString(): String =
        "TopicSnapshot(name=$name, state=$state, partitions=$partitions, metrics=$metrics, consumers=$consumers)"
}
