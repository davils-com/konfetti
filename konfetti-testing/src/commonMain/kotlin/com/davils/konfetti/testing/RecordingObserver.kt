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

import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.BusObserver
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update

/**
 * An observer that records every observation of a bus for a test to inspect.
 *
 * It keeps the first [capacity] observations in the order they arrived and counts the rest in [overflow], so a runaway
 * test cannot exhaust memory. Recording appends in constant time and is safe from any number of coroutines and
 * threads.
 *
 * @property capacity the most observations the observer keeps, between 1 and [EventRecorder.MAX_CAPACITY].
 * @constructor Creates an observer that keeps [EventRecorder.DEFAULT_CAPACITY] observations unless [capacity] says
 * otherwise.
 * @throws IllegalArgumentException if [capacity] is below 1 or above [EventRecorder.MAX_CAPACITY].
 * @since 1.0.0
 */
public class RecordingObserver(public val capacity: Int = EventRecorder.DEFAULT_CAPACITY) : BusObserver {
    init {
        require(capacity in 1..EventRecorder.MAX_CAPACITY) {
            "A recording observer needs a capacity between 1 and ${EventRecorder.MAX_CAPACITY}, but was $capacity"
        }
    }

    private val recorded = MutableStateFlow(Recording.empty<BusObservation>())

    /**
     * The recorded observations, in the order they arrived.
     *
     * @since 1.0.0
     */
    public val observations: List<BusObservation> get() = recorded.value.items

    /**
     * The observations that arrived after the observer was full and were not kept.
     *
     * @since 1.0.0
     */
    public val overflow: Long get() = recorded.value.overflow

    /**
     * Records [observation], or counts it in [overflow] when the observer is full.
     *
     * @param observation the observation.
     * @since 1.0.0
     */
    override fun observe(observation: BusObservation) {
        recorded.update { recording -> recording.with(observation, capacity) }
    }

    /**
     * Returns the recorded observations of type [O].
     *
     * @param O the type of the observations.
     * @return the observations of type [O], in the order they arrived.
     * @since 1.0.0
     */
    public inline fun <reified O : BusObservation> only(): List<O> = observations.filterIsInstance<O>()

    /**
     * Counts the recorded observations of type [O].
     *
     * @param O the type of the observations.
     * @return the number of observations of type [O].
     * @since 1.0.0
     */
    public inline fun <reified O : BusObservation> count(): Int = only<O>().size

    /**
     * Forgets every recorded observation and the overflow.
     *
     * @since 1.0.0
     */
    public fun clear() {
        recorded.value = Recording.empty()
    }

    /**
     * Shows the observer with the number of recorded and overflowed observations.
     *
     * @return the observer as text.
     * @since 1.0.0
     */
    override fun toString(): String = "RecordingObserver(recorded=${recorded.value.size}, overflow=$overflow)"
}
