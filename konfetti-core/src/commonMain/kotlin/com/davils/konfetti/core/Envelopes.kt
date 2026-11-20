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
import com.davils.konfetti.core.internal.EnvelopeRoute
import com.davils.konfetti.core.internal.lineage
import com.davils.konfetti.core.topic.OrderingKey
import com.davils.konfetti.core.topic.TopicName
import com.davils.kore.core.annotation.DavilsInternal
import com.davils.kore.core.identity.UuidV7
import com.davils.kore.core.principal.Principal

/**
 * Makes an envelope outside a bus, for the test support of konfetti.
 *
 * Marked [DavilsInternal] for the libraries of Davils: an envelope made here was never accepted by a bus, so it is
 * fit only for tests. Use `testEnvelope` of konfetti-testing instead.
 *
 * @param T the type of the event.
 * @param id the id of the envelope.
 * @param topic the topic of the envelope.
 * @param orderingKey the ordering key, or null.
 * @param causedBy the envelope that caused this one, or null.
 * @param headers the headers.
 * @param payload the event.
 * @param principal the principal, or null.
 * @return the envelope.
 * @since 1.0.0
 */
@DavilsInternal
@Suppress("LongParameterList")
public fun <T : Event> envelopeOf(
    id: UuidV7,
    topic: TopicName,
    orderingKey: OrderingKey?,
    causedBy: Envelope<Event>?,
    headers: EventHeaders,
    payload: T,
    principal: Principal?,
): Envelope<T> = Envelope(
    id = id,
    route = EnvelopeRoute(topic = topic, orderingKey = orderingKey),
    lineage = lineage(id, causedBy),
    headers = headers,
    payload = payload,
    principal = principal,
)
