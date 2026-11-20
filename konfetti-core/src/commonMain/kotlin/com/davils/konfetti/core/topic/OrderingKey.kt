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

import com.davils.konfetti.core.limit.BusLimits
import kotlin.jvm.JvmInline

/**
 * The key that keeps envelopes in order: envelopes of one topic with equal keys go to the same partition and reach
 * every consumer in the order they were published.
 *
 * A key is not empty and at most [BusLimits.HARD_ORDERING_KEY_LENGTH] characters long; a bus may allow less with
 * [BusLimits.maximumOrderingKeyLength]. The key decides only the partition, through a hash keyed with a secret of the
 * bus, so keys chosen by an attacker cannot crowd one partition on purpose.
 *
 * @property value the key.
 * @constructor Checks and wraps a key.
 * @throws IllegalArgumentException if [value] is empty or longer than [BusLimits.HARD_ORDERING_KEY_LENGTH].
 * @since 1.0.0
 */
@JvmInline
public value class OrderingKey(public val value: String) {
    init {
        require(value.isNotEmpty()) { "An ordering key must not be empty" }
        require(value.length <= BusLimits.HARD_ORDERING_KEY_LENGTH) {
            "An ordering key must be at most ${BusLimits.HARD_ORDERING_KEY_LENGTH} characters long, " +
                "but was ${value.length}"
        }
    }

    /**
     * Returns the key itself.
     *
     * @return [value].
     * @since 1.0.0
     */
    override fun toString(): String = value
}
