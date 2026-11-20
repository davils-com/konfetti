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

package com.davils.konfetti.core.bus

import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.konfetti.core.internal.bus.BusDefaults
import com.davils.konfetti.core.internal.bus.BusHooks
import com.davils.konfetti.core.internal.bus.BusSources
import com.davils.konfetti.core.limit.BusLimits
import com.davils.konfetti.core.topic.TopicConfiguration
import com.davils.kore.core.validation.Validatable
import com.davils.kore.core.validation.ValidationScope
import kotlin.time.Duration

/**
 * The validated settings of a bus, built by an [EventBusBuilder].
 *
 * The bus declares at most [BusLimits.maximumTopics] topics, each within the limits of the bus; it retains between 0
 * and [BusLimits.maximumRetain] dead letters; and its dead-letter stall is positive. Topic defaults changed after a
 * topic was declared draw a warning.
 *
 * @since 1.0.0
 */
public class EventBusConfiguration internal constructor(
    internal val topics: Map<String, TopicConfiguration<*>>,
    internal val deadLetterRetention: Int,
    internal val deadLetterStall: Duration,
    internal val limits: BusLimits,
    internal val hooks: BusHooks,
    internal val sources: BusSources,
    internal val defaults: BusDefaults,
    internal val hasLateTopicDefaults: Boolean,
) : Validatable {
    /**
     * Checks the settings against the limits of the bus.
     *
     * @receiver the validation that collects the violations.
     * @since 1.0.0
     */
    override fun ValidationScope.validate() {
        if (topics.size > limits.maximumTopics) {
            fail(
                ::topics,
                "declare ${topics.size} topics, but the bus holds at most ${limits.maximumTopics}",
                EventViolationCodes.TOO_MANY_TOPICS,
            )
        }
        each(::topics, topics) { topic -> with(topic) { tighterThan(limits) } }
        if (deadLetterRetention !in 0..limits.maximumRetain) {
            fail(
                ::deadLetterRetention,
                "must be between 0 and ${limits.maximumRetain}",
                EventViolationCodes.INVALID_RETENTION,
            )
        }
        if (!deadLetterStall.isPositive()) {
            fail(::deadLetterStall, "must be positive", EventViolationCodes.INVALID_STALL)
        }
        if (!hasLateTopicDefaults) return
        warn(
            property = defaults::topic,
            message = "were changed after a topic was declared and do not reach the topics declared before",
            code = EventViolationCodes.LATE_DEFAULT,
        )
    }
}
