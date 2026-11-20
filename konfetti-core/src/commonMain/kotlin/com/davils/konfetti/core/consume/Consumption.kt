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

import com.davils.konfetti.core.topic.TopicName
import com.davils.kore.core.validation.report.ValidationReport

/**
 * What became of an attempt to attach a consumer with [com.davils.konfetti.core.Topic.consume].
 *
 * @since 1.0.0
 */
public sealed interface Consumption {
    /**
     * Returns the attached consumer, or throws why there is none.
     *
     * @return the consumer.
     * @throws IllegalStateException if the name is taken, the topic no longer runs or holds its limit of consumers.
     * @throws IllegalArgumentException if the settings are invalid; the message is the rendered report.
     * @since 1.0.0
     */
    public fun getOrThrow(): Consumer

    /**
     * The consumer was attached and runs.
     *
     * @property consumer the consumer.
     * @since 1.0.0
     */
    public class Attached internal constructor(public val consumer: Consumer) : Consumption {
        /**
         * Returns [consumer].
         *
         * @return the consumer.
         * @since 1.0.0
         */
        override fun getOrThrow(): Consumer = consumer

        /**
         * Shows the consumption.
         *
         * @return the consumption as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Attached($consumer)"
    }

    /**
     * A consumer of the same name is attached to the topic; nothing was attached.
     *
     * @property topic the topic.
     * @property name the name that is taken.
     * @since 1.0.0
     */
    public class Duplicate internal constructor(public val topic: TopicName, public val name: ConsumerName) :
        Consumption {
        /**
         * Throws, since nothing was attached.
         *
         * @return never.
         * @throws IllegalStateException always.
         * @since 1.0.0
         */
        override fun getOrThrow(): Consumer = error("Consumer '$name' is already attached to topic '$topic'")

        /**
         * Shows the consumption.
         *
         * @return the consumption as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Duplicate($topic/$name)"
    }

    /**
     * The settings are invalid; nothing was attached.
     *
     * @property report the validation that failed.
     * @since 1.0.0
     */
    public class Invalid internal constructor(public val report: ValidationReport) : Consumption {
        /**
         * Throws, since nothing was attached.
         *
         * @return never.
         * @throws IllegalArgumentException always; the message is the rendered report.
         * @since 1.0.0
         */
        override fun getOrThrow(): Consumer = throw IllegalArgumentException(report.render())

        /**
         * Shows the consumption with the number of errors.
         *
         * @return the consumption as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Invalid(errors=${report.errors.size})"
    }

    /**
     * The topic no longer runs; nothing was attached.
     *
     * @property topic the topic.
     * @since 1.0.0
     */
    public class Closed internal constructor(public val topic: TopicName) : Consumption {
        /**
         * Throws, since nothing was attached.
         *
         * @return never.
         * @throws IllegalStateException always.
         * @since 1.0.0
         */
        override fun getOrThrow(): Consumer = error(
            "Topic '$topic' takes no new consumer because it is no longer running",
        )

        /**
         * Shows the consumption.
         *
         * @return the consumption as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Closed($topic)"
    }

    /**
     * The topic has [com.davils.konfetti.core.limit.BusLimits.maximumConsumers] consumers already; nothing was
     * attached.
     *
     * @property topic the topic.
     * @property limit the most consumers the topic may have.
     * @since 1.0.0
     */
    public class LimitReached internal constructor(public val topic: TopicName, public val limit: Int) : Consumption {
        /**
         * Throws, since nothing was attached.
         *
         * @return never.
         * @throws IllegalStateException always.
         * @since 1.0.0
         */
        override fun getOrThrow(): Consumer =
            error("Topic '$topic' takes no new consumer because it has its limit of $limit consumers")

        /**
         * Shows the consumption.
         *
         * @return the consumption as text.
         * @since 1.0.0
         */
        override fun toString(): String = "LimitReached($topic, limit=$limit)"
    }
}
