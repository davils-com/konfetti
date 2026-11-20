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

package com.davils.konfetti.testing

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.envelopeOf
import com.davils.konfetti.core.header.EventHeaders
import com.davils.konfetti.core.topic.OrderingKey
import com.davils.konfetti.core.topic.TopicName
import com.davils.kore.core.annotation.DavilsDsl
import com.davils.kore.core.annotation.DavilsInternal
import com.davils.kore.core.identity.UuidV7
import com.davils.kore.core.identity.generator.UuidGenerator
import com.davils.kore.core.principal.Principal
import com.davils.kore.testing.SequentialUuidV7Generator

/**
 * Sets what a test envelope made by [testEnvelope] carries besides its payload.
 *
 * @since 1.0.0
 */
@DavilsDsl
public class TestEnvelopeBuilder internal constructor() {
    /**
     * The topic of the envelope: `test.topic` by default.
     *
     * @since 1.0.0
     */
    public var topic: TopicName = TopicName("test.topic")

    /**
     * The ordering key of the envelope: none by default.
     *
     * @since 1.0.0
     */
    public var orderingKey: OrderingKey? = null

    /**
     * The headers of the envelope: none by default.
     *
     * @since 1.0.0
     */
    public var headers: EventHeaders = EventHeaders.EMPTY

    /**
     * The principal of the envelope: none by default.
     *
     * @since 1.0.0
     */
    public var principal: Principal? = null

    /**
     * The envelope that caused this one: none by default.
     *
     * @since 1.0.0
     */
    public var causedBy: Envelope<Event>? = null

    /**
     * The generator of the id of the envelope: a new [SequentialUuidV7Generator] by default.
     *
     * @since 1.0.0
     */
    public var identifiers: UuidGenerator<UuidV7> = SequentialUuidV7Generator()

    @OptIn(DavilsInternal::class)
    internal fun <T : Event> build(payload: T): Envelope<T> = envelopeOf(
        id = identifiers.next(),
        topic = topic,
        orderingKey = orderingKey,
        causedBy = causedBy,
        headers = headers,
        payload = payload,
        principal = principal,
    )
}
