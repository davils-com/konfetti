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

/**
 * Receives every observation of a bus, such as to export metrics.
 *
 * Observers are called synchronously, in the order they were registered, on the coroutine where the observed thing
 * happened: a publisher, a worker of a consumer or the coroutine that closes the bus. An observer must therefore be
 * fast and must not block or suspend; [ObservationStream] hands observations to collectors on their own coroutines.
 * An observer that throws is contained and counted in [BusSnapshot.observerFailures]; it never disturbs the bus.
 *
 * @since 1.0.0
 */
public fun interface BusObserver {
    /**
     * Whether the observer wants observations now; while it is false the bus does not call the observer, and while no
     * observer wants observations the bus does not even build them. A property that throws counts as false.
     *
     * @since 1.0.0
     */
    public val isObserving: Boolean get() = true

    /**
     * Receives one observation.
     *
     * @param observation the observation.
     * @since 1.0.0
     */
    public fun observe(observation: BusObservation)

    /**
     * The observer that observes nothing.
     *
     * @since 1.0.0
     */
    public companion object None : BusObserver {
        /**
         * Always false.
         *
         * @since 1.0.0
         */
        override val isObserving: Boolean get() = false

        /**
         * Ignores [observation].
         *
         * @param observation the observation.
         * @since 1.0.0
         */
        override fun observe(observation: BusObservation): Unit = Unit
    }
}
