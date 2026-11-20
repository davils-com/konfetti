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
import com.davils.konfetti.core.Topic
import com.davils.kore.core.validation.report.ValidationReport

/**
 * What became of a declaration of a topic at run time with [com.davils.konfetti.core.EventBus.declare].
 *
 * @param T the type of the events of the topic.
 * @since 1.0.0
 */
public sealed interface Declaration<T : Event> {
    /**
     * Returns the declared topic, or throws why there is none.
     *
     * @return the topic.
     * @throws IllegalStateException if the topic was already declared, the bus no longer runs or has reached its limit
     * of topics.
     * @throws IllegalArgumentException if the settings are invalid; the message is the rendered report.
     * @since 1.0.0
     */
    public fun getOrThrow(): Topic<T>

    /**
     * The topic was declared.
     *
     * @param T the type of the events of the topic.
     * @property topic the topic.
     * @property report the validation of the settings, with any warnings.
     * @since 1.0.0
     */
    public class Declared<T : Event> internal constructor(
        public val topic: Topic<T>,
        public val report: ValidationReport,
    ) : Declaration<T> {
        /**
         * Returns [topic].
         *
         * @return the topic.
         * @since 1.0.0
         */
        override fun getOrThrow(): Topic<T> = topic

        /**
         * Shows the declaration.
         *
         * @return the declaration as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Declared(${topic.name})"
    }

    /**
     * A topic of the same name exists; it was left as it is.
     *
     * @param T the type of the events of the topic.
     * @property name the name of the topic.
     * @since 1.0.0
     */
    public class AlreadyDeclared<T : Event> internal constructor(public val name: TopicName) : Declaration<T> {
        /**
         * Throws, since no topic was declared.
         *
         * @return never.
         * @throws IllegalStateException always.
         * @since 1.0.0
         */
        override fun getOrThrow(): Topic<T> = error("Topic '$name' is already declared on this bus")

        /**
         * Tells whether [other] reports the same outcome.
         *
         * @param other the object to compare with.
         * @return true if [other] is an equal outcome.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is AlreadyDeclared<*> && name == other.name

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = name.hashCode()

        /**
         * Shows the declaration.
         *
         * @return the declaration as text.
         * @since 1.0.0
         */
        override fun toString(): String = "AlreadyDeclared($name)"
    }

    /**
     * The settings are invalid; nothing was declared.
     *
     * @param T the type of the events of the topic.
     * @property report the validation that failed.
     * @since 1.0.0
     */
    public class Invalid<T : Event> internal constructor(public val report: ValidationReport) : Declaration<T> {
        /**
         * Throws, since no topic was declared.
         *
         * @return never.
         * @throws IllegalArgumentException always; the message is the rendered report.
         * @since 1.0.0
         */
        override fun getOrThrow(): Topic<T> = throw IllegalArgumentException(report.render())

        /**
         * Shows the declaration with the number of errors.
         *
         * @return the declaration as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Invalid(errors=${report.errors.size})"
    }

    /**
     * The bus no longer runs; nothing was declared.
     *
     * @param T the type of the events of the topic.
     * @property name the name of the topic.
     * @since 1.0.0
     */
    public class Closed<T : Event> internal constructor(public val name: TopicName) : Declaration<T> {
        /**
         * Throws, since no topic was declared.
         *
         * @return never.
         * @throws IllegalStateException always.
         * @since 1.0.0
         */
        override fun getOrThrow(): Topic<T> =
            error("Topic '$name' was not declared because the bus is no longer running")

        /**
         * Tells whether [other] reports the same outcome.
         *
         * @param other the object to compare with.
         * @return true if [other] is an equal outcome.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Closed<*> && name == other.name

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = name.hashCode()

        /**
         * Shows the declaration.
         *
         * @return the declaration as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Closed($name)"
    }

    /**
     * The bus holds [com.davils.konfetti.core.limit.BusLimits.maximumTopics] topics already; nothing was declared.
     *
     * @param T the type of the events of the topic.
     * @property name the name of the topic.
     * @property limit the most topics the bus may hold.
     * @since 1.0.0
     */
    public class LimitReached<T : Event> internal constructor(
        public val name: TopicName,
        public val limit: Int,
    ) : Declaration<T> {
        /**
         * Throws, since no topic was declared.
         *
         * @return never.
         * @throws IllegalStateException always.
         * @since 1.0.0
         */
        override fun getOrThrow(): Topic<T> =
            error("Topic '$name' was not declared because the bus holds its limit of $limit topics")

        /**
         * Tells whether [other] reports the same outcome.
         *
         * @param other the object to compare with.
         * @return true if [other] is an equal outcome.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean =
            other is LimitReached<*> && name == other.name && limit == other.limit

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = listOf(name, limit).hashCode()

        /**
         * Shows the declaration.
         *
         * @return the declaration as text.
         * @since 1.0.0
         */
        override fun toString(): String = "LimitReached($name, limit=$limit)"
    }
}
