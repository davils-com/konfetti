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

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.topic.TopicName

private const val HASH_FACTOR: Int = 31

/**
 * What became of a publication: an accepted envelope or a rejection.
 *
 * @param T the type of the event.
 * @since 1.0.0
 */
public sealed interface PublishResult<out T : Event> {
    /**
     * The bus accepted the publication; every consumer of the topic accounts for [envelope] exactly once.
     *
     * @param T the type of the event.
     * @property envelope the envelope of the event.
     * @since 1.0.0
     */
    public class Published<out T : Event> internal constructor(public val envelope: Envelope<T>) : PublishResult<T> {
        /**
         * Tells whether [other] published an equal envelope.
         *
         * @param other the object to compare with.
         * @return true if [other] is a publication of an envelope with the same id.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Published<*> && envelope == other.envelope

        /**
         * Returns the hash of the envelope.
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = envelope.hashCode()

        /**
         * Shows the result with the envelope.
         *
         * @return the result as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Published($envelope)"
    }

    /**
     * The bus refused the publication; no envelope exists and no consumer sees the event.
     *
     * @property topic the topic of the publication.
     * @property reason why the bus refused it.
     * @since 1.0.0
     */
    public class Rejected internal constructor(
        public val topic: TopicName,
        public val reason: Rejection,
    ) : PublishResult<Nothing> {
        /**
         * Tells whether [other] is a rejection on the same topic for the same reason.
         *
         * @param other the object to compare with.
         * @return true if [other] is an equal rejection.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Rejected && topic == other.topic && reason == other.reason

        /**
         * Returns a hash of the topic and the reason.
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = HASH_FACTOR * topic.hashCode() + reason.hashCode()

        /**
         * Shows the rejection as `Rejected(topic, reason)`.
         *
         * @return the rejection as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Rejected($topic, $reason)"
    }
}
