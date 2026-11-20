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

import com.davils.konfetti.core.lifecycle.BusState

/**
 * The state and counts of a bus, its topics and their consumers at one moment.
 *
 * @property state the state of the bus.
 * @property topics every topic in the order it was declared, the dead letters last.
 * @property observerFailures the calls of observers that threw.
 * @property interceptorFailures the calls of publish interceptors that threw.
 * @since 1.0.0
 */
public class BusSnapshot internal constructor(
    public val state: BusState,
    public val topics: List<TopicSnapshot>,
    public val observerFailures: Long,
    public val interceptorFailures: Long,
) {
    private val fields: List<Any> get() = listOf(state, topics, observerFailures, interceptorFailures)

    /**
     * Tells whether [other] is an equal snapshot.
     *
     * @param other the object to compare with.
     * @return true if [other] holds the same state and counts.
     * @since 1.0.0
     */
    override fun equals(other: Any?): Boolean = other is BusSnapshot && fields == other.fields

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
    override fun toString(): String = "BusSnapshot(state=$state, topics=$topics, observerFailures=$observerFailures, " +
        "interceptorFailures=$interceptorFailures)"
}
