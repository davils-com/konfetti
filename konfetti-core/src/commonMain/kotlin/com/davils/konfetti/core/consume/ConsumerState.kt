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

package com.davils.konfetti.core.consume

import com.davils.konfetti.core.internal.displayName

/**
 * The state of a consumer.
 *
 * A consumer moves from [Running] to [Draining] to [Detached], or from [Running] straight to [Detached] or [Stopped];
 * it never comes back.
 *
 * @since 1.0.0
 */
public sealed interface ConsumerState {
    /**
     * The consumer receives and handles envelopes.
     *
     * @since 1.0.0
     */
    public object Running : ConsumerState {
        /**
         * Returns `Running`.
         *
         * @return the name of the state.
         * @since 1.0.0
         */
        override fun toString(): String = "Running"
    }

    /**
     * The consumer receives nothing new and handles what it has received.
     *
     * @since 1.0.0
     */
    public object Draining : ConsumerState {
        /**
         * Returns `Draining`.
         *
         * @return the name of the state.
         * @since 1.0.0
         */
        override fun toString(): String = "Draining"
    }

    /**
     * The consumer was detached, by itself, by its owner, by a drain or by the end of the bus.
     *
     * @since 1.0.0
     */
    public object Detached : ConsumerState {
        /**
         * Returns `Detached`.
         *
         * @return the name of the state.
         * @since 1.0.0
         */
        override fun toString(): String = "Detached"
    }

    /**
     * The consumer stopped because of [com.davils.konfetti.core.failure.FailurePolicy.Stop].
     *
     * @property cause the failure that stopped it.
     * @since 1.0.0
     */
    public class Stopped internal constructor(public val cause: Throwable) : ConsumerState {
        /**
         * Tells whether [other] stopped for the same failure.
         *
         * @param other the object to compare with.
         * @return true if [other] is a stop with an equal cause.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Stopped && cause == other.cause

        /**
         * Returns the hash of the cause.
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = cause.hashCode()

        /**
         * Shows the state with the type of the failure.
         *
         * @return the state as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Stopped(${cause::class.displayName})"
    }
}
