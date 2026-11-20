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
 * Why an accepted envelope was dropped instead of handled.
 *
 * A dropped envelope is settled: it is counted once, in [ConsumerMetrics.dropped] for a consumer or in
 * [TopicMetrics.dropped] when no consumer had received it, and observed as [BusObservation.Dropped].
 *
 * @since 1.0.0
 */
public enum class DropReason {
    /**
     * The mailbox of a consumer with [com.davils.konfetti.core.consume.OverflowPolicy.DropOldest] or
     * [com.davils.konfetti.core.consume.OverflowPolicy.DropLatest] was full.
     *
     * @since 1.0.0
     */
    Overflow,

    /**
     * The mailbox of a consumer with [com.davils.konfetti.core.consume.OverflowPolicy.Backpressure] stayed full longer
     * than the longest stall of the consumer allows.
     *
     * @since 1.0.0
     */
    Stalled,

    /**
     * The consumer was detached before it handled the envelope.
     *
     * @since 1.0.0
     */
    Detached,

    /**
     * The consumer stopped because of [com.davils.konfetti.core.failure.FailurePolicy.Stop].
     *
     * @since 1.0.0
     */
    Stopped,

    /**
     * The bus or the topic was closed before the envelope was handled, or the dead letters of the bus no longer ran.
     *
     * @since 1.0.0
     */
    Closed,

    /**
     * A drain timed out before the envelope was handled.
     *
     * @since 1.0.0
     */
    Abandoned,

    /**
     * The envelope failed and its dead letter found no room in the dead letters within the dead-letter stall of the
     * bus.
     *
     * @since 1.0.0
     */
    DeadLetterOverflow,
}
