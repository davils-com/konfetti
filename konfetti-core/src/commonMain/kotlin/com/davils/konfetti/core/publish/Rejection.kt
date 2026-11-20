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

package com.davils.konfetti.core.publish

/**
 * Why a bus refused a publication.
 *
 * @since 1.0.0
 */
public enum class Rejection {
    /**
     * The topic no longer takes publications because the bus is draining or closed.
     *
     * @since 1.0.0
     */
    Closed,

    /**
     * The partition had no room, for [com.davils.konfetti.core.Topic.tryPublish] and
     * [com.davils.konfetti.core.Topic.offer], or stayed full longer than a dead letter may wait.
     *
     * @since 1.0.0
     */
    Overflow,

    /**
     * The ordering key of the topic failed for the event: the function threw, or returned an empty key or one longer
     * than [com.davils.konfetti.core.limit.BusLimits.maximumOrderingKeyLength].
     *
     * @since 1.0.0
     */
    InvalidOrderingKey,

    /**
     * The headers, after every publish interceptor, exceed [com.davils.konfetti.core.limit.BusLimits.maximumHeaders]
     * or [com.davils.konfetti.core.limit.BusLimits.maximumHeaderValueLength].
     *
     * @since 1.0.0
     */
    OversizedHeaders,
}
