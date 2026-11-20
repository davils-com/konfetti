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

/**
 * What a consumer does when the mailbox of a worker is full and its topic hands it another envelope.
 *
 * @since 1.0.0
 */
public enum class OverflowPolicy {
    /**
     * Wait for room; the default. Nothing is lost, but the partition waits, so every consumer of the partition and,
     * once its buffer is full, every publisher of the partition slows down to the pace of this consumer. A finite
     * `maximumStall` of the consumer bounds the wait: an envelope that finds no room in time is dropped for this
     * consumer with [com.davils.konfetti.core.observe.DropReason.Stalled].
     *
     * @since 1.0.0
     */
    Backpressure,

    /**
     * Drop the oldest envelope of the mailbox to make room; the consumer keeps up with the newest envelopes.
     *
     * @since 1.0.0
     */
    DropOldest,

    /**
     * Drop the new envelope; the consumer keeps what it has.
     *
     * @since 1.0.0
     */
    DropLatest,
}
