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

package com.davils.konfetti.core.failure

import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.konfetti.core.limit.BusLimits
import com.davils.kore.core.validation.Validatable
import com.davils.kore.core.validation.ValidationScope

private const val HASH_FACTOR: Int = 31

/**
 * What a consumer does with an envelope its handler failed: the handler threw, timed out, or an interceptor failed.
 *
 * A [Terminal] policy decides at once; [Retry] delivers the envelope again and decides with its terminal policy when
 * the attempts are used up. Whatever the policy, the envelope is settled exactly once.
 *
 * @since 1.0.0
 */
public sealed interface FailurePolicy {
    /**
     * A policy that decides without another attempt.
     *
     * @since 1.0.0
     */
    public sealed interface Terminal : FailurePolicy

    /**
     * Leave the envelope and go on with the next; the envelope is counted as skipped.
     *
     * @since 1.0.0
     */
    public object Skip : Terminal {
        /**
         * Returns `Skip`.
         *
         * @return the name of the policy.
         * @since 1.0.0
         */
        override fun toString(): String = "Skip"
    }

    /**
     * Stop the consumer: the envelope and every envelope after it are dropped as stopped, and the state of the consumer
     * becomes [com.davils.konfetti.core.consume.ConsumerState.Stopped] with the failure.
     *
     * @since 1.0.0
     */
    public object Stop : Terminal {
        /**
         * Returns `Stop`.
         *
         * @return the name of the policy.
         * @since 1.0.0
         */
        override fun toString(): String = "Stop"
    }

    /**
     * Publish a [DeadLetter] of the envelope to the dead letters of the bus and go on with the next; the default.
     *
     * The dead letter waits at most the dead-letter stall of the bus for room; an envelope whose dead letter finds no
     * room in time, or whose bus no longer runs, is dropped with the reason it could not be stored for.
     *
     * @since 1.0.0
     */
    public object DeadLetter : Terminal {
        /**
         * Returns `DeadLetter`.
         *
         * @return the name of the policy.
         * @since 1.0.0
         */
        override fun toString(): String = "DeadLetter"
    }

    /**
     * Deliver the envelope again, up to [attempts] attempts in all, waiting as [backoff] says before each retry, and
     * decide with [then] when no attempt succeeded.
     *
     * @property attempts the attempts in all, the first one included, at least 1.
     * @property backoff the wait before each retry.
     * @property then what to do when every attempt failed.
     * @since 1.0.0
     */
    public class Retry internal constructor(
        public val attempts: Int,
        public val backoff: Backoff,
        public val then: Terminal,
    ) : FailurePolicy, Validatable {
        /**
         * Checks that [attempts] lies between 1 and [BusLimits.HARD_ATTEMPTS] and that the backoff is valid.
         *
         * @receiver the validation that collects the violations.
         * @since 1.0.0
         */
        override fun ValidationScope.validate() {
            if (attempts !in 1..BusLimits.HARD_ATTEMPTS) {
                fail(::attempts, "must be between 1 and ${BusLimits.HARD_ATTEMPTS}", EventViolationCodes.INVALID_RETRY)
            }
            within(::backoff) { with(backoff) { validate() } }
        }

        /**
         * Tells whether [other] is a retry with the same attempts, backoff and terminal policy.
         *
         * @param other the object to compare with.
         * @return true if [other] is an equal policy.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean =
            other is Retry && attempts == other.attempts && backoff == other.backoff && then == other.then

        /**
         * Returns a hash of the attempts, the backoff and the terminal policy.
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = HASH_FACTOR * (HASH_FACTOR * attempts + backoff.hashCode()) + then.hashCode()

        /**
         * Shows the policy as `Retry(attempts, backoff, then)`.
         *
         * @return the policy as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Retry(attempts=$attempts, backoff=$backoff, then=$then)"
    }
}
