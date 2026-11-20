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
import kotlin.time.Duration

private const val HASH_FACTOR: Int = 31

/**
 * How long a consumer waits before it delivers an envelope again after a failed attempt.
 *
 * The wait suspends the worker of the consumer, so envelopes behind the failing one wait as well and keep their order.
 * Backoffs are made with [fixed] and [exponential] and validated when the consumer is attached: no delay is negative
 * or longer than [BusLimits.maximumRetryDelay].
 *
 * @since 1.0.0
 */
public sealed interface Backoff : Validatable {
    /**
     * The same delay before every retry.
     *
     * @property delay the delay; zero retries at once.
     * @since 1.0.0
     */
    public class Fixed internal constructor(public val delay: Duration) : Backoff {
        /**
         * Checks that the delay is neither negative nor above [BusLimits.HARD_RETRY_DELAY].
         *
         * @receiver the validation that collects the violations.
         * @since 1.0.0
         */
        override fun ValidationScope.validate() {
            if (delay.isNegative()) fail(::delay, "must not be negative", EventViolationCodes.INVALID_BACKOFF)
            if (delay <= BusLimits.HARD_RETRY_DELAY) return
            fail(::delay, "must not exceed ${BusLimits.HARD_RETRY_DELAY}", EventViolationCodes.INVALID_BACKOFF)
        }

        /**
         * Tells whether [other] is a fixed backoff with the same delay.
         *
         * @param other the object to compare with.
         * @return true if [other] is an equal backoff.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Fixed && delay == other.delay

        /**
         * Returns the hash of the delay.
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = delay.hashCode()

        /**
         * Shows the backoff as `Fixed(delay)`.
         *
         * @return the backoff as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Fixed($delay)"
    }

    /**
     * A delay that starts at [initial] and grows by [factor] with every retry up to [max], optionally with full
     * jitter: a random delay between zero and the grown delay, which spreads retries of many consumers apart.
     *
     * @property initial the delay before the first retry.
     * @property max the longest delay.
     * @property factor what each delay is multiplied by for the next retry, at least 1.
     * @property hasJitter whether the delay is randomized.
     * @since 1.0.0
     */
    public class Exponential internal constructor(
        public val initial: Duration,
        public val max: Duration,
        public val factor: Double,
        public val hasJitter: Boolean,
    ) : Backoff {
        private val fields: List<Any> get() = listOf(initial, max, factor, hasJitter)

        /**
         * Checks that the initial delay is positive, the longest delay is not shorter and not above
         * [BusLimits.HARD_RETRY_DELAY], and the factor is a finite number of at least 1.
         *
         * @receiver the validation that collects the violations.
         * @since 1.0.0
         */
        override fun ValidationScope.validate() {
            if (!initial.isPositive()) fail(::initial, "must be positive", EventViolationCodes.INVALID_BACKOFF)
            if (max < initial) {
                fail(::max, "must not be shorter than the initial delay", EventViolationCodes.INVALID_BACKOFF)
            }
            if (max > BusLimits.HARD_RETRY_DELAY) {
                fail(::max, "must not exceed ${BusLimits.HARD_RETRY_DELAY}", EventViolationCodes.INVALID_BACKOFF)
            }
            if (factor.isFinite() && factor >= 1.0) return
            fail(::factor, "must be a finite number of at least 1", EventViolationCodes.INVALID_BACKOFF)
        }

        /**
         * Tells whether [other] is an exponential backoff with the same delays, factor and jitter.
         *
         * @param other the object to compare with.
         * @return true if [other] is an equal backoff.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Exponential && fields == other.fields

        /**
         * Returns a hash of the delays, the factor and the jitter.
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = fields.fold(0) { hash, field -> HASH_FACTOR * hash + field.hashCode() }

        /**
         * Shows the backoff as `Exponential(initial, max, factor, jitter)`.
         *
         * @return the backoff as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Exponential(initial=$initial, max=$max, factor=$factor, jitter=$hasJitter)"
    }
}
