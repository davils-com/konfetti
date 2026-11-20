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

import com.davils.konfetti.core.Event
import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.konfetti.core.limit.BusLimits
import com.davils.kore.core.validation.Validatable
import com.davils.kore.core.validation.ValidationScope
import kotlin.reflect.KClass

/**
 * The validated settings of one topic, built by a [TopicBuilder].
 *
 * The settings are checked against the limits of the bus: the partitions lie between 1 and
 * [BusLimits.maximumPartitions], the buffer between 1 and [BusLimits.maximumBufferCapacity], the retention between 0
 * and [BusLimits.maximumRetain], and the name is not [TopicName.DEAD_LETTERS]. A topic with several partitions but no
 * ordering key draws a warning, because its envelopes lose their order.
 *
 * @since 1.0.0
 */
public class TopicConfiguration<T : Event> internal constructor(
    internal val name: TopicName,
    internal val eventType: KClass<T>,
    internal val partitions: Int,
    internal val bufferCapacity: Int,
    internal val retain: Int,
    internal val orderBy: ((T) -> String)?,
    private val limits: BusLimits,
) : Validatable {
    /**
     * Checks the settings against the limits of the bus.
     *
     * @receiver the validation that collects the violations.
     * @since 1.0.0
     */
    override fun ValidationScope.validate() {
        if (name == TopicName.DEAD_LETTERS) {
            fail(::name, "is reserved for the dead letters of the bus", EventViolationCodes.RESERVED_NAME)
        }
        within(limits)
        if (partitions <= 1 || orderBy != null) return
        warn(
            property = ::partitions,
            message = "spreads envelopes over $partitions partitions without orderBy, so they lose their order",
            code = EventViolationCodes.UNORDERED_PARTITIONS,
        )
    }

    internal fun ValidationScope.tighterThan(bus: BusLimits) {
        if (partitions > bus.maximumPartitions && partitions <= limits.maximumPartitions) {
            fail(::partitions, "must be between 1 and ${bus.maximumPartitions}", EventViolationCodes.INVALID_PARTITIONS)
        }
        if (bufferCapacity > bus.maximumBufferCapacity && bufferCapacity <= limits.maximumBufferCapacity) {
            fail(
                ::bufferCapacity,
                "must be between 1 and ${bus.maximumBufferCapacity}",
                EventViolationCodes.INVALID_BUFFER,
            )
        }
        if (retain <= bus.maximumRetain || retain > limits.maximumRetain) return
        fail(::retain, "must be between 0 and ${bus.maximumRetain}", EventViolationCodes.INVALID_RETENTION)
    }

    private fun ValidationScope.within(bounds: BusLimits) {
        if (partitions !in 1..bounds.maximumPartitions) {
            fail(
                ::partitions,
                "must be between 1 and ${bounds.maximumPartitions}",
                EventViolationCodes.INVALID_PARTITIONS,
            )
        }
        if (bufferCapacity !in 1..bounds.maximumBufferCapacity) {
            fail(
                ::bufferCapacity,
                "must be between 1 and ${bounds.maximumBufferCapacity}",
                EventViolationCodes.INVALID_BUFFER,
            )
        }
        if (retain in 0..bounds.maximumRetain) return
        fail(::retain, "must be between 0 and ${bounds.maximumRetain}", EventViolationCodes.INVALID_RETENTION)
    }
}
