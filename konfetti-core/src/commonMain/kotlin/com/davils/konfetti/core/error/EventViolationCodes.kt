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

package com.davils.konfetti.core.error

import com.davils.kore.core.validation.violation.ViolationCode

/**
 * The codes of the violations konfetti reports when it validates a bus, a topic, a consumer or limits.
 *
 * The codes are a contract: a code keeps its meaning across versions, so tools may match on it.
 *
 * @since 1.0.0
 */
public object EventViolationCodes {
    /**
     * A name does not follow its grammar, such as a consumer name with upper-case letters.
     *
     * @since 1.0.0
     */
    public val INVALID_NAME: ViolationCode = ViolationCode("konfetti.event.invalid-name")

    /**
     * The number of partitions of a topic is below 1 or above the limit of the bus.
     *
     * @since 1.0.0
     */
    public val INVALID_PARTITIONS: ViolationCode = ViolationCode("konfetti.event.invalid-partitions")

    /**
     * The buffer of a topic is below 1 or above the limit of the bus.
     *
     * @since 1.0.0
     */
    public val INVALID_BUFFER: ViolationCode = ViolationCode("konfetti.event.invalid-buffer")

    /**
     * A retention is negative or above the limit of the bus.
     *
     * @since 1.0.0
     */
    public val INVALID_RETENTION: ViolationCode = ViolationCode("konfetti.event.invalid-retention")

    /**
     * A warning: a topic spreads envelopes over several partitions without an ordering key, so they lose their order.
     *
     * @since 1.0.0
     */
    public val UNORDERED_PARTITIONS: ViolationCode = ViolationCode("konfetti.event.unordered-partitions")

    /**
     * The concurrency of a consumer is below 1 or above the number of partitions of its topic.
     *
     * @since 1.0.0
     */
    public val INVALID_CONCURRENCY: ViolationCode = ViolationCode("konfetti.event.invalid-concurrency")

    /**
     * The mailbox of a consumer is below 1 or above the limit of the bus.
     *
     * @since 1.0.0
     */
    public val INVALID_MAILBOX: ViolationCode = ViolationCode("konfetti.event.invalid-mailbox")

    /**
     * A consumer declares no handler.
     *
     * @since 1.0.0
     */
    public val MISSING_HANDLER: ViolationCode = ViolationCode("konfetti.event.missing-handler")

    /**
     * A retry policy allows fewer than 1 or more attempts than the limit of the bus.
     *
     * @since 1.0.0
     */
    public val INVALID_RETRY: ViolationCode = ViolationCode("konfetti.event.invalid-retry")

    /**
     * A backoff has a negative, infinite or too long delay, or a factor below 1.
     *
     * @since 1.0.0
     */
    public val INVALID_BACKOFF: ViolationCode = ViolationCode("konfetti.event.invalid-backoff")

    /**
     * A handling timeout is not positive or above the limit of the bus.
     *
     * @since 1.0.0
     */
    public val INVALID_TIMEOUT: ViolationCode = ViolationCode("konfetti.event.invalid-timeout")

    /**
     * The longest stall of a consumer or of the dead letters is not positive.
     *
     * @since 1.0.0
     */
    public val INVALID_STALL: ViolationCode = ViolationCode("konfetti.event.invalid-stall")

    /**
     * A consumer of the dead letters would send what fails back to the dead letters.
     *
     * @since 1.0.0
     */
    public val DEAD_LETTER_LOOP: ViolationCode = ViolationCode("konfetti.event.dead-letter-loop")

    /**
     * A topic takes the name reserved for the dead letters of the bus.
     *
     * @since 1.0.0
     */
    public val RESERVED_NAME: ViolationCode = ViolationCode("konfetti.event.reserved-name")

    /**
     * A warning: the topic defaults of a bus were changed after a topic was declared, so they miss that topic.
     *
     * @since 1.0.0
     */
    public val LATE_DEFAULT: ViolationCode = ViolationCode("konfetti.event.late-default")

    /**
     * A bus declares more topics than its limits allow.
     *
     * @since 1.0.0
     */
    public val TOO_MANY_TOPICS: ViolationCode = ViolationCode("konfetti.event.too-many-topics")

    /**
     * A limit is below 1, or a duration limit is not positive.
     *
     * @since 1.0.0
     */
    public val NOT_POSITIVE: ViolationCode = ViolationCode("konfetti.limit.not-positive")

    /**
     * A limit is above its hard ceiling.
     *
     * @since 1.0.0
     */
    public val ABOVE_HARD_LIMIT: ViolationCode = ViolationCode("konfetti.limit.above-hard-limit")
}
