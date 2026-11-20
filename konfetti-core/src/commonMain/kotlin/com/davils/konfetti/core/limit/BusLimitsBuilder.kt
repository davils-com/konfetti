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

import com.davils.kore.core.annotation.DavilsDsl
import com.davils.kore.core.dsl.ValidatingBuilder
import kotlin.time.Duration

/**
 * Builds [BusLimits]; every property starts from the limits [busLimits] starts from.
 *
 * Each property documents its limit on [BusLimits]. A property can no longer be set once the builder was evaluated.
 *
 * @since 1.0.0
 */
@DavilsDsl
public class BusLimitsBuilder internal constructor() : ValidatingBuilder<BusLimits>() {
    override val subject: String get() = "bus limits"

    /**
     * See [BusLimits.maximumTopics].
     *
     * @since 1.0.0
     */
    public var maximumTopics: Int = BusLimits.DEFAULT_TOPIC_COUNT
        set(value) {
            checkOpen()
            field = value
        }

    /**
     * See [BusLimits.maximumConsumers].
     *
     * @since 1.0.0
     */
    public var maximumConsumers: Int = BusLimits.DEFAULT_CONSUMER_COUNT
        set(value) {
            checkOpen()
            field = value
        }

    /**
     * See [BusLimits.maximumPartitions].
     *
     * @since 1.0.0
     */
    public var maximumPartitions: Int = BusLimits.HARD_PARTITIONS
        set(value) {
            checkOpen()
            field = value
        }

    /**
     * See [BusLimits.maximumBufferCapacity].
     *
     * @since 1.0.0
     */
    public var maximumBufferCapacity: Int = BusLimits.HARD_BUFFER_CAPACITY
        set(value) {
            checkOpen()
            field = value
        }

    /**
     * See [BusLimits.maximumMailboxCapacity].
     *
     * @since 1.0.0
     */
    public var maximumMailboxCapacity: Int = BusLimits.HARD_MAILBOX_CAPACITY
        set(value) {
            checkOpen()
            field = value
        }

    /**
     * See [BusLimits.maximumRetain].
     *
     * @since 1.0.0
     */
    public var maximumRetain: Int = BusLimits.HARD_RETAIN
        set(value) {
            checkOpen()
            field = value
        }

    /**
     * See [BusLimits.maximumAttempts].
     *
     * @since 1.0.0
     */
    public var maximumAttempts: Int = BusLimits.DEFAULT_ATTEMPT_COUNT
        set(value) {
            checkOpen()
            field = value
        }

    /**
     * See [BusLimits.maximumHeaders].
     *
     * @since 1.0.0
     */
    public var maximumHeaders: Int = BusLimits.DEFAULT_HEADER_COUNT
        set(value) {
            checkOpen()
            field = value
        }

    /**
     * See [BusLimits.maximumHeaderValueLength].
     *
     * @since 1.0.0
     */
    public var maximumHeaderValueLength: Int = BusLimits.DEFAULT_HEADER_LENGTH
        set(value) {
            checkOpen()
            field = value
        }

    /**
     * See [BusLimits.maximumOrderingKeyLength].
     *
     * @since 1.0.0
     */
    public var maximumOrderingKeyLength: Int = BusLimits.DEFAULT_KEY_LENGTH
        set(value) {
            checkOpen()
            field = value
        }

    /**
     * See [BusLimits.maximumHandlingTimeout].
     *
     * @since 1.0.0
     */
    public var maximumHandlingTimeout: Duration = BusLimits.DEFAULT_HANDLING_TIMEOUT
        set(value) {
            checkOpen()
            field = value
        }

    /**
     * See [BusLimits.maximumRetryDelay].
     *
     * @since 1.0.0
     */
    public var maximumRetryDelay: Duration = BusLimits.DEFAULT_RETRY_DELAY
        set(value) {
            checkOpen()
            field = value
        }

    internal fun from(base: BusLimits) {
        maximumTopics = base.maximumTopics
        maximumConsumers = base.maximumConsumers
        maximumPartitions = base.maximumPartitions
        maximumBufferCapacity = base.maximumBufferCapacity
        maximumMailboxCapacity = base.maximumMailboxCapacity
        maximumRetain = base.maximumRetain
        maximumAttempts = base.maximumAttempts
        maximumHeaders = base.maximumHeaders
        maximumHeaderValueLength = base.maximumHeaderValueLength
        maximumOrderingKeyLength = base.maximumOrderingKeyLength
        maximumHandlingTimeout = base.maximumHandlingTimeout
        maximumRetryDelay = base.maximumRetryDelay
    }

    internal fun ceilings() {
        maximumTopics = BusLimits.HARD_TOPICS
        maximumConsumers = BusLimits.HARD_CONSUMERS
        maximumPartitions = BusLimits.HARD_PARTITIONS
        maximumBufferCapacity = BusLimits.HARD_BUFFER_CAPACITY
        maximumMailboxCapacity = BusLimits.HARD_MAILBOX_CAPACITY
        maximumRetain = BusLimits.HARD_RETAIN
        maximumAttempts = BusLimits.HARD_ATTEMPTS
        maximumHeaders = BusLimits.HARD_HEADERS
        maximumHeaderValueLength = BusLimits.HARD_HEADER_VALUE_LENGTH
        maximumOrderingKeyLength = BusLimits.HARD_ORDERING_KEY_LENGTH
        maximumHandlingTimeout = BusLimits.HARD_HANDLING_TIMEOUT
        maximumRetryDelay = BusLimits.HARD_RETRY_DELAY
    }

    override fun assemble(): BusLimits = BusLimits(this)
}
