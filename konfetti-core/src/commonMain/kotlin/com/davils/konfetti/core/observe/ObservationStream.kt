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

import com.davils.konfetti.core.internal.platform.guard
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onSubscription

/**
 * An observer that hands the observations of a bus to any number of collectors, each on its own coroutine.
 *
 * The stream holds the last [capacity] observations for collectors that fall behind and drops the oldest beyond that,
 * so a slow collector never slows the bus. Every collector learns what it missed: [sequenced] numbers the
 * observations and tells each collector how many it missed. While no one collects, the stream observes nothing.
 *
 * @property capacity the observations held for collectors that fall behind, between 1 and [MAX_CAPACITY].
 * @constructor Creates a stream that holds [DEFAULT_CAPACITY] observations unless [capacity] says otherwise.
 * @throws IllegalArgumentException if [capacity] is below 1 or above [MAX_CAPACITY].
 * @since 1.0.0
 */
public class ObservationStream(public val capacity: Int = DEFAULT_CAPACITY) : BusObserver {
    init {
        require(capacity in 1..MAX_CAPACITY) {
            "An observation stream needs a capacity between 1 and $MAX_CAPACITY, but was $capacity"
        }
    }

    private val shared = MutableSharedFlow<Numbered>(
        extraBufferCapacity = capacity,
        onBufferOverflow = BufferOverflow.DROP_OLDEST,
    )
    private val emission = guard()
    private var sequence: Long = 0

    /**
     * Whether anyone collects the stream now.
     *
     * @since 1.0.0
     */
    override val isObserving: Boolean get() = shared.subscriptionCount.value > 0

    /**
     * Hands [observation] to every collector, if anyone collects.
     *
     * @param observation the observation.
     * @since 1.0.0
     */
    override fun observe(observation: BusObservation) {
        if (!isObserving) return
        emission.locked {
            sequence += 1
            shared.tryEmit(Numbered(sequence, observation))
        }
    }

    /**
     * Returns the observations from the moment of collection on, each with its number and what the collector missed
     * before it.
     *
     * @return a flow that never completes; cancel its collector to stop.
     * @since 1.0.0
     */
    public fun sequenced(): Flow<SequencedObservation> = flow {
        var last = -1L
        val subscribed = shared.onSubscription { last = emission.locked { sequence } }
        subscribed.collect { numbered ->
            val missed = (numbered.sequence - last - 1).coerceAtLeast(0)
            last = maxOf(a = last, b = numbered.sequence)
            emit(SequencedObservation(numbered.sequence, missed, numbered.observation))
        }
    }

    /**
     * Returns the observations from the moment of collection on.
     *
     * @return a flow that never completes; cancel its collector to stop.
     * @since 1.0.0
     */
    public fun observations(): Flow<BusObservation> = sequenced().map { sequenced -> sequenced.observation }

    private data class Numbered(val sequence: Long, val observation: BusObservation)

    /**
     * The capacities of a stream.
     *
     * @since 1.0.0
     */
    public companion object {
        /**
         * The capacity of a stream that names none: 256.
         *
         * @since 1.0.0
         */
        public const val DEFAULT_CAPACITY: Int = 256

        /**
         * The largest capacity of a stream: 65,536.
         *
         * @since 1.0.0
         */
        public const val MAX_CAPACITY: Int = 65_536
    }
}
