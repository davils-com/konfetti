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

package com.davils.konfetti.core.internal

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.kore.core.identity.UuidV7

internal data class EnvelopeLineage(val correlationId: UuidV7, val causationId: UuidV7?)

internal fun lineage(id: UuidV7, cause: Envelope<Event>?): EnvelopeLineage {
    if (cause == null) return EnvelopeLineage(correlationId = id, causationId = null)
    return EnvelopeLineage(correlationId = cause.correlationId, causationId = cause.id)
}
