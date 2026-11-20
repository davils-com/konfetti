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

/**
 * Makes an envelope of [payload] without a bus, such as to call a handler directly in a test.
 *
 * @param T the type of the event.
 * @param payload the event.
 * @param block sets the topic, key, headers, principal, cause and id of the envelope.
 * @return the envelope.
 * @since 1.0.0
 */
public fun <T : Event> testEnvelope(payload: T, block: TestEnvelopeBuilder.() -> Unit = {}): Envelope<T> {
    val builder = TestEnvelopeBuilder()
    builder.block()
    return builder.build(payload)
}
