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

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.consume.ConsumerName
import com.davils.konfetti.core.internal.displayName
import com.davils.konfetti.core.lifecycle.BusState
import com.davils.konfetti.core.publish.Rejection
import com.davils.konfetti.core.topic.TopicName
import kotlin.time.Duration

/**
 * Something that happened on a bus, as a [BusObserver] receives it.
 *
 * Observations carry envelopes and failures as they are, so an observer can read their headers and ids; their text
 * shows only ids, names, counts and the types of failures, never payloads or the messages of failures.
 *
 * @since 1.0.0
 */
public sealed interface BusObservation {
    /**
     * The bus changed its state.
     *
     * @property state the new state.
     * @since 1.0.0
     */
    public class StateChanged internal constructor(public val state: BusState) : BusObservation {
        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is StateChanged && state == other.state

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = state.hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "StateChanged($state)"
    }

    /**
     * A topic was declared.
     *
     * @property topic the topic.
     * @since 1.0.0
     */
    public class TopicDeclared internal constructor(public val topic: TopicName) : BusObservation {
        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is TopicDeclared && topic == other.topic

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = topic.hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "TopicDeclared($topic)"
    }

    /**
     * A publication was accepted.
     *
     * @property envelope the envelope of the publication.
     * @since 1.0.0
     */
    public class Published internal constructor(public val envelope: Envelope<Event>) : BusObservation {
        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Published && envelope == other.envelope

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = envelope.hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Published(${envelope.id} on ${envelope.topic})"
    }

    /**
     * A publication was rejected.
     *
     * @property topic the topic of the publication.
     * @property reason why it was rejected.
     * @since 1.0.0
     */
    public class Rejected internal constructor(public val topic: TopicName, public val reason: Rejection) :
        BusObservation {
        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Rejected && topic == other.topic && reason == other.reason

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = listOf(topic, reason).hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Rejected($topic, $reason)"
    }

    /**
     * A handler of a consumer finished an envelope.
     *
     * @property envelope the envelope.
     * @property consumer the consumer.
     * @property attempt the attempt that succeeded, starting at 1.
     * @property duration how long the attempt took, interceptors included.
     * @since 1.0.0
     */
    public class Delivered internal constructor(
        public val envelope: Envelope<Event>,
        public val consumer: ConsumerName,
        public val attempt: Int,
        public val duration: Duration,
    ) : BusObservation {
        private val fields: List<Any> get() = listOf(envelope, consumer, attempt, duration)

        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Delivered && fields == other.fields

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = fields.hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Delivered(${envelope.id} to $consumer, attempt=$attempt, duration=$duration)"
    }

    /**
     * A predicate or an interceptor of a consumer kept an envelope from its handler.
     *
     * @property envelope the envelope.
     * @property consumer the consumer.
     * @since 1.0.0
     */
    public class Filtered internal constructor(
        public val envelope: Envelope<Event>,
        public val consumer: ConsumerName,
    ) :
        BusObservation {
        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean =
            other is Filtered && envelope == other.envelope && consumer == other.consumer

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = listOf(envelope, consumer).hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Filtered(${envelope.id} from $consumer)"
    }

    /**
     * An attempt of a consumer failed.
     *
     * @property envelope the envelope.
     * @property consumer the consumer.
     * @property attempt the attempt that failed, starting at 1.
     * @property cause the failure.
     * @since 1.0.0
     */
    public class Failed internal constructor(
        public val envelope: Envelope<Event>,
        public val consumer: ConsumerName,
        public val attempt: Int,
        public val cause: Throwable,
    ) : BusObservation {
        private val fields: List<Any> get() = listOf(envelope, consumer, attempt, cause)

        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Failed && fields == other.fields

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = fields.hashCode()

        /**
         * Shows the observation with the type of the failure.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String =
            "Failed(${envelope.id} in $consumer, attempt=$attempt, cause=${cause::class.displayName})"
    }

    /**
     * A consumer will deliver an envelope again after a delay.
     *
     * @property envelope the envelope.
     * @property consumer the consumer.
     * @property attempt the attempt that comes next.
     * @property delay the wait before it.
     * @since 1.0.0
     */
    public class Retrying internal constructor(
        public val envelope: Envelope<Event>,
        public val consumer: ConsumerName,
        public val attempt: Int,
        public val delay: Duration,
    ) : BusObservation {
        private val fields: List<Any> get() = listOf(envelope, consumer, attempt, delay)

        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Retrying && fields == other.fields

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = fields.hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Retrying(${envelope.id} in $consumer, attempt=$attempt, delay=$delay)"
    }

    /**
     * A consumer gave up an envelope with [com.davils.konfetti.core.failure.FailurePolicy.Skip].
     *
     * @property envelope the envelope.
     * @property consumer the consumer.
     * @property attempts the attempts that failed.
     * @since 1.0.0
     */
    public class Skipped internal constructor(
        public val envelope: Envelope<Event>,
        public val consumer: ConsumerName,
        public val attempts: Int,
    ) : BusObservation {
        private val fields: List<Any> get() = listOf(envelope, consumer, attempts)

        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Skipped && fields == other.fields

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = fields.hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Skipped(${envelope.id} in $consumer, attempts=$attempts)"
    }

    /**
     * A consumer moved an envelope to the dead letters of the bus.
     *
     * @property envelope the envelope.
     * @property consumer the consumer.
     * @property attempts the attempts that failed.
     * @since 1.0.0
     */
    public class DeadLettered internal constructor(
        public val envelope: Envelope<Event>,
        public val consumer: ConsumerName,
        public val attempts: Int,
    ) : BusObservation {
        private val fields: List<Any> get() = listOf(envelope, consumer, attempts)

        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is DeadLettered && fields == other.fields

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = fields.hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "DeadLettered(${envelope.id} in $consumer, attempts=$attempts)"
    }

    /**
     * An accepted envelope was dropped.
     *
     * @property envelope the envelope.
     * @property consumer the consumer that dropped it, or null when it was dropped before any consumer received it.
     * @property reason why it was dropped.
     * @since 1.0.0
     */
    public class Dropped internal constructor(
        public val envelope: Envelope<Event>,
        public val consumer: ConsumerName?,
        public val reason: DropReason,
    ) : BusObservation {
        private val fields: List<Any?> get() = listOf(envelope, consumer, reason)

        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Dropped && fields == other.fields

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = fields.hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Dropped(${envelope.id} by ${consumer ?: "topic"}, reason=$reason)"
    }

    /**
     * A consumer was attached to a topic.
     *
     * @property topic the topic.
     * @property consumer the consumer.
     * @since 1.0.0
     */
    public class ConsumerAttached internal constructor(public val topic: TopicName, public val consumer: ConsumerName) :
        BusObservation {
        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean =
            other is ConsumerAttached && topic == other.topic && consumer == other.consumer

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = listOf(topic, consumer).hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "ConsumerAttached($topic/$consumer)"
    }

    /**
     * A consumer stopped because of [com.davils.konfetti.core.failure.FailurePolicy.Stop].
     *
     * @property topic the topic.
     * @property consumer the consumer.
     * @property cause the failure that stopped it.
     * @since 1.0.0
     */
    public class ConsumerStopped internal constructor(
        public val topic: TopicName,
        public val consumer: ConsumerName,
        public val cause: Throwable,
    ) : BusObservation {
        private val fields: List<Any> get() = listOf(topic, consumer, cause)

        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is ConsumerStopped && fields == other.fields

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = fields.hashCode()

        /**
         * Shows the observation with the type of the failure.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "ConsumerStopped($topic/$consumer, cause=${cause::class.displayName})"
    }

    /**
     * A consumer was detached from its topic, by itself, by its owner or by a drain.
     *
     * @property topic the topic.
     * @property consumer the consumer.
     * @since 1.0.0
     */
    public class ConsumerDetached internal constructor(public val topic: TopicName, public val consumer: ConsumerName) :
        BusObservation {
        /**
         * Tells whether [other] is an equal observation.
         *
         * @param other the object to compare with.
         * @return true if [other] observed the same.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean =
            other is ConsumerDetached && topic == other.topic && consumer == other.consumer

        /**
         * Returns a hash consistent with [equals].
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = listOf(topic, consumer).hashCode()

        /**
         * Shows the observation.
         *
         * @return the observation as text.
         * @since 1.0.0
         */
        override fun toString(): String = "ConsumerDetached($topic/$consumer)"
    }
}
