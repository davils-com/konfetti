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

package com.davils.konfetti.core.limit

import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.kore.core.validation.Validatable
import com.davils.kore.core.validation.ValidationScope
import com.davils.kore.core.validation.validateOrThrow
import kotlin.reflect.KProperty0
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours

private const val SUBJECT: String = "bus limits"
private const val DEFAULT_TOPICS: Int = 1_024
private const val DEFAULT_CONSUMERS: Int = 256
private const val DEFAULT_ATTEMPTS: Int = 100
private const val DEFAULT_HEADERS: Int = 64
private const val DEFAULT_HEADER_VALUE_LENGTH: Int = 4_096
private const val DEFAULT_ORDERING_KEY_LENGTH: Int = 1_024
private const val HASH_FACTOR: Int = 31

/**
 * The limits that bound what one event bus holds and what one publication may carry.
 *
 * Every structure of a bus that grows with its use has a limit here: the topics of the bus, the consumers of a topic,
 * the partitions, buffers, mailboxes and retentions, the attempts of a retry, the headers of an envelope, the length
 * of an ordering key and the longest timeout and retry delay. The bus checks a limit before it does the work, so what
 * exceeds a limit is refused with a validation report, a refused declaration or a rejected publication, and never
 * grows memory first.
 *
 * Every count is at least 1 and every duration is positive, and none exceeds its hard ceiling, a `HARD_*` constant of
 * the companion that no configuration can raise. [Default] is the limits of a bus that sets none; other limits are
 * built with [busLimits], which validates them. The limits validate themselves when they are created, however they
 * are created, so no limits beyond a ceiling exist.
 *
 * The limits are an immutable value and safe to share between threads; two limits are equal when every limit is
 * equal.
 *
 * @since 1.0.0
 */
public class BusLimits internal constructor(builder: BusLimitsBuilder) : Validatable {
    /**
     * The most topics a bus may declare, not counting its dead letters: 1,024 by default, at most [HARD_TOPICS].
     *
     * A bus whose builder declares more topics fails to build; a declaration at run time beyond the limit returns
     * [com.davils.konfetti.core.topic.Declaration.LimitReached].
     *
     * @since 1.0.0
     */
    public val maximumTopics: Int = builder.maximumTopics

    /**
     * The most consumers one topic may have at the same time: 256 by default, at most [HARD_CONSUMERS].
     *
     * A consumer beyond the limit is refused with [com.davils.konfetti.core.consume.Consumption.LimitReached]; the
     * flows of [com.davils.konfetti.core.Topic.events] count as consumers.
     *
     * @since 1.0.0
     */
    public val maximumConsumers: Int = builder.maximumConsumers

    /**
     * The most partitions of a topic: 1,024 by default, at most [HARD_PARTITIONS].
     *
     * @since 1.0.0
     */
    public val maximumPartitions: Int = builder.maximumPartitions

    /**
     * The largest buffer of a partition, in envelopes: 1,048,576 by default, at most [HARD_BUFFER_CAPACITY].
     *
     * @since 1.0.0
     */
    public val maximumBufferCapacity: Int = builder.maximumBufferCapacity

    /**
     * The largest mailbox of a consumer worker, in envelopes: 1,048,576 by default, at most [HARD_MAILBOX_CAPACITY].
     *
     * @since 1.0.0
     */
    public val maximumMailboxCapacity: Int = builder.maximumMailboxCapacity

    /**
     * The most envelopes a topic, the dead letters included, may retain for consumers that start from the retained
     * envelopes: 65,536 by default, at most [HARD_RETAIN].
     *
     * @since 1.0.0
     */
    public val maximumRetain: Int = builder.maximumRetain

    /**
     * The most attempts a retry policy may allow: 100 by default, at most [HARD_ATTEMPTS].
     *
     * @since 1.0.0
     */
    public val maximumAttempts: Int = builder.maximumAttempts

    /**
     * The most headers one envelope may carry after the publish interceptors enriched it: 64 by default, at most
     * [HARD_HEADERS].
     *
     * A publication with more headers is rejected with [com.davils.konfetti.core.publish.Rejection.OversizedHeaders].
     *
     * @since 1.0.0
     */
    public val maximumHeaders: Int = builder.maximumHeaders

    /**
     * The most characters of the encoded value of one header: 4,096 by default, at most [HARD_HEADER_VALUE_LENGTH].
     *
     * A publication with a longer value is rejected with
     * [com.davils.konfetti.core.publish.Rejection.OversizedHeaders].
     *
     * @since 1.0.0
     */
    public val maximumHeaderValueLength: Int = builder.maximumHeaderValueLength

    /**
     * The most characters of an ordering key: 1,024 by default, at most [HARD_ORDERING_KEY_LENGTH].
     *
     * A publication whose key is longer is rejected with
     * [com.davils.konfetti.core.publish.Rejection.InvalidOrderingKey].
     *
     * @since 1.0.0
     */
    public val maximumOrderingKeyLength: Int = builder.maximumOrderingKeyLength

    /**
     * The longest handling timeout a consumer may set: one day by default, at most [HARD_HANDLING_TIMEOUT].
     *
     * A consumer without a timeout is not bounded by it.
     *
     * @since 1.0.0
     */
    public val maximumHandlingTimeout: Duration = builder.maximumHandlingTimeout

    /**
     * The longest delay before a retry, of a fixed backoff and of the cap of an exponential backoff: one hour by
     * default, at most [HARD_RETRY_DELAY].
     *
     * @since 1.0.0
     */
    public val maximumRetryDelay: Duration = builder.maximumRetryDelay

    init {
        validateOrThrow(SUBJECT)
    }

    private val counts: List<Bound>
        get() = listOf(
            Bound(property = ::maximumTopics, value = maximumTopics, ceiling = HARD_TOPICS),
            Bound(property = ::maximumConsumers, value = maximumConsumers, ceiling = HARD_CONSUMERS),
            Bound(property = ::maximumPartitions, value = maximumPartitions, ceiling = HARD_PARTITIONS),
            Bound(property = ::maximumBufferCapacity, value = maximumBufferCapacity, ceiling = HARD_BUFFER_CAPACITY),
            Bound(property = ::maximumMailboxCapacity, value = maximumMailboxCapacity, ceiling = HARD_MAILBOX_CAPACITY),
            Bound(property = ::maximumRetain, value = maximumRetain, ceiling = HARD_RETAIN),
            Bound(property = ::maximumAttempts, value = maximumAttempts, ceiling = HARD_ATTEMPTS),
            Bound(property = ::maximumHeaders, value = maximumHeaders, ceiling = HARD_HEADERS),
            Bound(
                property = ::maximumHeaderValueLength,
                value = maximumHeaderValueLength,
                ceiling = HARD_HEADER_VALUE_LENGTH,
            ),
            Bound(
                property = ::maximumOrderingKeyLength,
                value = maximumOrderingKeyLength,
                ceiling = HARD_ORDERING_KEY_LENGTH,
            ),
        )

    private val durations: List<DurationBound>
        get() = listOf(
            DurationBound(
                property = ::maximumHandlingTimeout,
                value = maximumHandlingTimeout,
                ceiling = HARD_HANDLING_TIMEOUT,
            ),
            DurationBound(
                property = ::maximumRetryDelay,
                value = maximumRetryDelay,
                ceiling = HARD_RETRY_DELAY,
            ),
        )

    /**
     * Checks that every count is at least 1, every duration is positive and nothing exceeds its hard ceiling.
     *
     * @receiver the validation that collects the violations.
     * @since 1.0.0
     */
    override fun ValidationScope.validate() {
        counts.forEach { bound -> bounded(bound) }
        durations.forEach { bound -> bounded(bound) }
    }

    /**
     * Tells whether [other] is limits with the same value for every limit.
     *
     * @param other the object to compare with.
     * @return true if [other] is equal limits.
     * @since 1.0.0
     */
    override fun equals(other: Any?): Boolean {
        if (other !is BusLimits) return false
        return counts.map(Bound::value) == other.counts.map(Bound::value) &&
            durations.map(DurationBound::value) == other.durations.map(DurationBound::value)
    }

    /**
     * Returns a hash of every limit, consistent with [equals].
     *
     * @return the hash.
     * @since 1.0.0
     */
    override fun hashCode(): Int = HASH_FACTOR *
        counts.map(Bound::value).hashCode() +
        durations.map(DurationBound::value).hashCode()

    /**
     * Shows every limit, such as `BusLimits(topics=1024, consumers=256, …)`.
     *
     * @return the limits as text.
     * @since 1.0.0
     */
    override fun toString(): String =
        "BusLimits(topics=$maximumTopics, consumers=$maximumConsumers, partitions=$maximumPartitions, " +
            "buffer=$maximumBufferCapacity, mailbox=$maximumMailboxCapacity, retain=$maximumRetain, " +
            "attempts=$maximumAttempts, headers=$maximumHeaders, headerValue=$maximumHeaderValueLength, " +
            "orderingKey=$maximumOrderingKeyLength, handlingTimeout=$maximumHandlingTimeout, " +
            "retryDelay=$maximumRetryDelay)"

    private fun ValidationScope.bounded(bound: Bound) {
        val value = bound.value
        if (value < 1) fail(bound.property, "must be at least 1, but was $value", EventViolationCodes.NOT_POSITIVE)
        if (value <= bound.ceiling) return
        fail(bound.property, "must not exceed ${bound.ceiling}, but was $value", EventViolationCodes.ABOVE_HARD_LIMIT)
    }

    private fun ValidationScope.bounded(bound: DurationBound) {
        val value = bound.value
        if (!value.isPositive()) {
            fail(
                bound.property,
                "must be positive, but was $value",
                EventViolationCodes.NOT_POSITIVE,
            )
        }
        if (value <= bound.ceiling) return
        fail(bound.property, "must not exceed ${bound.ceiling}, but was $value", EventViolationCodes.ABOVE_HARD_LIMIT)
    }

    private data class Bound(val property: KProperty0<*>, val value: Int, val ceiling: Int)

    private data class DurationBound(val property: KProperty0<*>, val value: Duration, val ceiling: Duration)

    /**
     * The hard ceilings of the limits and the default limits.
     *
     * @since 1.0.0
     */
    public companion object {
        /**
         * The hard ceiling of [maximumTopics]: 65,536.
         *
         * @since 1.0.0
         */
        public const val HARD_TOPICS: Int = 65_536

        /**
         * The hard ceiling of [maximumConsumers]: 4,096.
         *
         * @since 1.0.0
         */
        public const val HARD_CONSUMERS: Int = 4_096

        /**
         * The hard ceiling of [maximumPartitions]: 1,024.
         *
         * @since 1.0.0
         */
        public const val HARD_PARTITIONS: Int = 1_024

        /**
         * The hard ceiling of [maximumBufferCapacity]: 1,048,576.
         *
         * @since 1.0.0
         */
        public const val HARD_BUFFER_CAPACITY: Int = 1_048_576

        /**
         * The hard ceiling of [maximumMailboxCapacity]: 1,048,576.
         *
         * @since 1.0.0
         */
        public const val HARD_MAILBOX_CAPACITY: Int = 1_048_576

        /**
         * The hard ceiling of [maximumRetain]: 65,536.
         *
         * @since 1.0.0
         */
        public const val HARD_RETAIN: Int = 65_536

        /**
         * The hard ceiling of [maximumAttempts]: 1,000.
         *
         * @since 1.0.0
         */
        public const val HARD_ATTEMPTS: Int = 1_000

        /**
         * The hard ceiling of [maximumHeaders], and the most headers any
         * [com.davils.konfetti.core.header.EventHeaders] hold: 1,024.
         *
         * @since 1.0.0
         */
        public const val HARD_HEADERS: Int = 1_024

        /**
         * The hard ceiling of [maximumHeaderValueLength], and the longest encoded value any
         * [com.davils.konfetti.core.header.EventHeaders] hold: 1,048,576 characters.
         *
         * @since 1.0.0
         */
        public const val HARD_HEADER_VALUE_LENGTH: Int = 1_048_576

        /**
         * The hard ceiling of [maximumOrderingKeyLength], and the longest
         * [com.davils.konfetti.core.topic.OrderingKey]: 65,536 characters.
         *
         * @since 1.0.0
         */
        public const val HARD_ORDERING_KEY_LENGTH: Int = 65_536

        /**
         * The hard ceiling of [maximumHandlingTimeout]: seven days.
         *
         * @since 1.0.0
         */
        public val HARD_HANDLING_TIMEOUT: Duration = 7.days

        /**
         * The hard ceiling of [maximumRetryDelay]: one day.
         *
         * @since 1.0.0
         */
        public val HARD_RETRY_DELAY: Duration = 1.days

        internal const val DEFAULT_TOPIC_COUNT: Int = DEFAULT_TOPICS
        internal const val DEFAULT_CONSUMER_COUNT: Int = DEFAULT_CONSUMERS
        internal const val DEFAULT_ATTEMPT_COUNT: Int = DEFAULT_ATTEMPTS
        internal const val DEFAULT_HEADER_COUNT: Int = DEFAULT_HEADERS
        internal const val DEFAULT_HEADER_LENGTH: Int = DEFAULT_HEADER_VALUE_LENGTH
        internal const val DEFAULT_KEY_LENGTH: Int = DEFAULT_ORDERING_KEY_LENGTH
        internal val DEFAULT_HANDLING_TIMEOUT: Duration = 1.days
        internal val DEFAULT_RETRY_DELAY: Duration = 1.hours

        /**
         * The limits of a bus that sets none, as each limit describes.
         *
         * @since 1.0.0
         */
        public val Default: BusLimits = BusLimits(BusLimitsBuilder())

        internal val Ceiling: BusLimits = BusLimits(BusLimitsBuilder().apply { ceilings() })
    }
}
