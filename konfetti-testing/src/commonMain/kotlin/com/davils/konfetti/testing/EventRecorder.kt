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

package com.davils.konfetti.testing

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update

/**
 * Records the envelopes a handler receives, such as with `handle(recorder::record)`, and lets a test wait for them.
 *
 * The recorder keeps the first [capacity] envelopes in the order they arrived and counts the rest in [overflow], so a
 * runaway test cannot exhaust memory. Recording appends in constant time and is safe from any number of coroutines and
 * threads.
 *
 * @param T the type of the events.
 * @property capacity the most envelopes the recorder keeps, between 1 and [MAX_CAPACITY].
 * @constructor Creates a recorder that keeps [DEFAULT_CAPACITY] envelopes unless [capacity] says otherwise.
 * @throws IllegalArgumentException if [capacity] is below 1 or above [MAX_CAPACITY].
 * @since 1.0.0
 */
public class EventRecorder<T : Event>(public val capacity: Int = DEFAULT_CAPACITY) {
    init {
        require(capacity in 1..MAX_CAPACITY) {
            "A recorder needs a capacity between 1 and $MAX_CAPACITY, but was $capacity"
        }
    }

    private val recorded = MutableStateFlow(Recording.empty<Envelope<T>>())

    /**
     * The recorded envelopes, in the order they arrived.
     *
     * @since 1.0.0
     */
    public val envelopes: List<Envelope<T>> get() = recorded.value.items

    /**
     * The payloads of the recorded envelopes, in the order they arrived.
     *
     * @since 1.0.0
     */
    public val payloads: List<T> get() = envelopes.map { envelope -> envelope.payload }

    /**
     * The envelopes that arrived after the recorder was full and were not kept.
     *
     * @since 1.0.0
     */
    public val overflow: Long get() = recorded.value.overflow

    /**
     * Records [envelope], or counts it in [overflow] when the recorder is full.
     *
     * @param envelope the envelope.
     * @since 1.0.0
     */
    public fun record(envelope: Envelope<T>) {
        recorded.update { recording -> recording.with(envelope, capacity) }
    }

    /**
     * Waits until at least [count] envelopes were recorded.
     *
     * @param count the envelopes to wait for, at most [capacity].
     * @return the recorded envelopes.
     * @throws IllegalArgumentException if [count] exceeds [capacity], since the recorder could never hold them.
     * @since 1.0.0
     */
    public suspend fun awaitCount(count: Int): List<Envelope<T>> {
        require(count <= capacity) { "A recorder of capacity $capacity never holds $count envelopes" }
        return recorded.first { recording -> recording.size >= count }.items
    }

    /**
     * Shows the recorder with the number of recorded and overflowed envelopes.
     *
     * @return the recorder as text.
     * @since 1.0.0
     */
    override fun toString(): String = "EventRecorder(recorded=${recorded.value.size}, overflow=$overflow)"

    /**
     * The capacities of a recorder.
     *
     * @since 1.0.0
     */
    public companion object {
        /**
         * The capacity of a recorder that names none: 10,000.
         *
         * @since 1.0.0
         */
        public const val DEFAULT_CAPACITY: Int = 10_000

        /**
         * The largest capacity of a recorder: 1,048,576.
         *
         * @since 1.0.0
         */
        public const val MAX_CAPACITY: Int = 1_048_576
    }
}
