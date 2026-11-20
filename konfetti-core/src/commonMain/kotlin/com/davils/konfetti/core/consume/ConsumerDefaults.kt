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

import com.davils.konfetti.core.failure.FailurePolicy
import com.davils.konfetti.core.internal.delivery.ConsumerSettings
import com.davils.kore.core.annotation.DavilsDsl
import kotlin.time.Duration

private const val DEFAULT_MAILBOX_CAPACITY: Int = 64

/**
 * The settings every consumer of a bus starts from, set in the `consumerDefaults` block of the bus builder.
 *
 * @since 1.0.0
 */
@DavilsDsl
public class ConsumerDefaults internal constructor() {
    /**
     * The envelopes the mailbox of each worker holds: 64 by default.
     *
     * @since 1.0.0
     */
    public var mailboxCapacity: Int = DEFAULT_MAILBOX_CAPACITY

    /**
     * Where a consumer starts: [StartPosition.Latest] by default.
     *
     * @since 1.0.0
     */
    public var start: StartPosition = StartPosition.Latest

    /**
     * What a consumer does when a mailbox is full: [OverflowPolicy.Backpressure] by default.
     *
     * @since 1.0.0
     */
    public var overflow: OverflowPolicy = OverflowPolicy.Backpressure

    /**
     * How long a full mailbox may hold up its partition under [OverflowPolicy.Backpressure]: without limit by default.
     *
     * @since 1.0.0
     */
    public var maximumStall: Duration = Duration.INFINITE

    /**
     * How long one attempt of a handler may take: without limit by default.
     *
     * @since 1.0.0
     */
    public var handlingTimeout: Duration? = null

    /**
     * What a consumer does with an envelope it failed: [FailurePolicy.DeadLetter] by default.
     *
     * @since 1.0.0
     */
    public var onFailure: FailurePolicy = FailurePolicy.DeadLetter

    internal fun snapshot(): ConsumerSettings = ConsumerSettings(
        mailboxCapacity = mailboxCapacity,
        start = start,
        overflow = overflow,
        maximumStall = maximumStall,
        handlingTimeout = handlingTimeout,
        onFailure = onFailure,
    )
}
