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

package com.davils.konfetti.core

import com.davils.konfetti.core.header.EventHeaders
import com.davils.konfetti.core.internal.EnvelopeLineage
import com.davils.konfetti.core.internal.EnvelopeRoute
import com.davils.konfetti.core.internal.displayName
import com.davils.konfetti.core.internal.shown
import com.davils.konfetti.core.topic.OrderingKey
import com.davils.konfetti.core.topic.TopicName
import com.davils.kore.core.identity.UuidV7
import com.davils.kore.core.principal.Principal
import kotlin.time.Instant

private const val MAX_SHOWN_KEY_LENGTH: Int = 64
private const val MAX_SHOWN_PRINCIPAL_LENGTH: Int = 64

/**
 * An event on its way through a bus, with what the bus knows about it.
 *
 * The bus makes an envelope for every accepted publication: it gives it a time-ordered [id], records the topic, the
 * ordering key, the headers after every publish interceptor, the principal the publication was attributed to, and the
 * envelope that caused it. Envelopes of one causal chain share a [correlationId], the id of the envelope that started
 * the chain.
 *
 * Two envelopes are equal when their ids are equal. An envelope is immutable and safe to share between threads; its
 * payload is as safe as the event it carries.
 *
 * @since 1.0.0
 */
public class Envelope<out T : Event> internal constructor(
    /**
     * The identifier of the envelope, a UUID of version 7 whose time is [occurredAt].
     *
     * @since 1.0.0
     */
    public val id: UuidV7,
    private val route: EnvelopeRoute,
    private val lineage: EnvelopeLineage,
    /**
     * The headers of the envelope, after every publish interceptor.
     *
     * @since 1.0.0
     */
    public val headers: EventHeaders,
    /**
     * The event.
     *
     * @since 1.0.0
     */
    public val payload: T,
    /**
     * The principal the publication was attributed to, or null for an anonymous publication.
     *
     * @since 1.0.0
     */
    public val principal: Principal?,
) {
    /**
     * The name of the topic the event was published on.
     *
     * @since 1.0.0
     */
    public val topic: TopicName get() = route.topic

    /**
     * The key that decided the partition of the envelope, or null when the topic has no ordering key.
     *
     * @since 1.0.0
     */
    public val orderingKey: OrderingKey? get() = route.orderingKey

    /**
     * The id of the envelope that started the causal chain of this envelope; its own [id] when nothing caused it.
     *
     * @since 1.0.0
     */
    public val correlationId: UuidV7 get() = lineage.correlationId

    /**
     * The id of the envelope that caused this envelope, or null when nothing caused it.
     *
     * @since 1.0.0
     */
    public val causationId: UuidV7? get() = lineage.causationId

    /**
     * The instant the bus accepted the publication, taken from the time of [id], to the millisecond.
     *
     * @since 1.0.0
     */
    public val occurredAt: Instant get() = id.instant

    /**
     * Whether another envelope caused this envelope.
     *
     * @since 1.0.0
     */
    public val isCaused: Boolean get() = causationId != null

    /**
     * Tells whether [other] is an envelope with the same id.
     *
     * @param other the object to compare with.
     * @return true if [other] is an envelope with an equal [id].
     * @since 1.0.0
     */
    override fun equals(other: Any?): Boolean = other is Envelope<*> && id == other.id

    /**
     * Returns the hash of [id].
     *
     * @return the hash.
     * @since 1.0.0
     */
    override fun hashCode(): Int = id.hashCode()

    /**
     * Shows the envelope without its payload: the id, the topic, the ordering key, the cause, the headers with
     * sensitive values replaced by `REDACTED`, the id of the principal and the type of the payload. Every text that
     * came from a caller is escaped and shortened, so the text is safe to show.
     *
     * @return the envelope as text.
     * @since 1.0.0
     */
    override fun toString(): String {
        val parts = buildList {
            add("id=$id")
            add("topic=$topic")
            orderingKey?.let { key -> add("orderingKey=${shown(key.value, MAX_SHOWN_KEY_LENGTH)}") }
            causationId?.let { cause -> add("causationId=$cause") }
            if (!headers.isEmpty) add("headers=$headers")
            principal?.let { caller -> add("principal=${shown(caller.id, MAX_SHOWN_PRINCIPAL_LENGTH)}") }
            add("payload=${payload::class.displayName}")
        }
        return parts.joinToString(prefix = "Envelope(", postfix = ")")
    }
}
