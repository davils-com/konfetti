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
 * Where a new consumer starts in the envelopes of its topic.
 *
 * @since 1.0.0
 */
public enum class StartPosition {
    /**
     * With the envelopes the topic hands on after the consumer is attached; the default.
     *
     * @since 1.0.0
     */
    Latest,

    /**
     * With the envelopes the topic retains, in the order they were published, followed without gap or repetition by
     * the envelopes after them.
     *
     * @since 1.0.0
     */
    Retained,
}
