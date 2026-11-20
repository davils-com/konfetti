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

package com.davils.konfetti.core.topic

import com.davils.konfetti.core.internal.partition.TopicSettings
import com.davils.kore.core.annotation.DavilsDsl

private const val DEFAULT_BUFFER_CAPACITY: Int = 64

/**
 * The settings every topic of a bus starts from, set in the `topicDefaults` block of the bus builder.
 *
 * A topic takes the defaults as they are when it is declared; a later change does not reach it, and the bus reports a
 * warning with [com.davils.konfetti.core.error.EventViolationCodes.LATE_DEFAULT] for such a change.
 *
 * @since 1.0.0
 */
@DavilsDsl
public class TopicDefaults internal constructor() {
    /**
     * The partitions of a topic: 1 by default.
     *
     * @since 1.0.0
     */
    public var partitions: Int = 1

    /**
     * The envelopes each partition buffers before a publisher waits: 64 by default.
     *
     * @since 1.0.0
     */
    public var bufferCapacity: Int = DEFAULT_BUFFER_CAPACITY

    /**
     * The envelopes a topic retains for consumers that start from the retained envelopes: none by default.
     *
     * @since 1.0.0
     */
    public var retain: Int = 0

    internal fun snapshot(): TopicSettings =
        TopicSettings(partitions = partitions, bufferCapacity = bufferCapacity, retain = retain)
}
