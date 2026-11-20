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

package com.davils.konfetti.core.lifecycle

/**
 * How draining a bus or a consumer ended.
 *
 * @since 1.0.0
 */
public sealed interface DrainResult {
    /**
     * Every accepted envelope was settled by every consumer in time.
     *
     * @since 1.0.0
     */
    public object Completed : DrainResult {
        /**
         * Returns `Completed`.
         *
         * @return the name of the result.
         * @since 1.0.0
         */
        override fun toString(): String = "Completed"
    }

    /**
     * The timeout passed first; what was still on its way was dropped as abandoned.
     *
     * @property abandoned the envelopes the drain dropped, counted as a [CloseReport] counts them.
     * @since 1.0.0
     */
    public class TimedOut internal constructor(public val abandoned: Long) : DrainResult {
        /**
         * Tells whether [other] reports the same abandoned envelopes.
         *
         * @param other the object to compare with.
         * @return true if [other] is a timeout with an equal [abandoned].
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is TimedOut && abandoned == other.abandoned

        /**
         * Returns the hash of [abandoned].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = abandoned.hashCode()

        /**
         * Shows the result as `TimedOut(abandoned=n)`.
         *
         * @return the result as text.
         * @since 1.0.0
         */
        override fun toString(): String = "TimedOut(abandoned=$abandoned)"
    }

    /**
     * Nothing was drained: the bus was closed before the drain began or by a close during it, or the consumer was
     * already detached or stopped.
     *
     * @since 1.0.0
     */
    public object AlreadyClosed : DrainResult {
        /**
         * Returns `AlreadyClosed`.
         *
         * @return the name of the result.
         * @since 1.0.0
         */
        override fun toString(): String = "AlreadyClosed"
    }
}
