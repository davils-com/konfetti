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

package com.davils.konfetti.core.internal.publish

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.currentEnvelope
import com.davils.konfetti.core.header.EventHeaders
import com.davils.konfetti.core.internal.EnvelopeRoute
import com.davils.konfetti.core.internal.lineage
import com.davils.konfetti.core.internal.partition.PartitionHandoff
import com.davils.konfetti.core.internal.partition.TopicRuntime
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.publish.Attribution
import com.davils.konfetti.core.publish.Publication
import com.davils.konfetti.core.publish.PublishResult
import com.davils.konfetti.core.publish.Rejection
import com.davils.konfetti.core.topic.OrderingKey
import com.davils.kore.core.failure.contained
import com.davils.kore.core.principal.Principal
import com.davils.kore.core.principal.currentPrincipal
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.currentCoroutineContext
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration

internal class Publisher<T : Event>(private val runtime: TopicRuntime<T>) {
    private val environment get() = runtime.environment

    suspend fun publish(
        event: T,
        headers: EventHeaders,
        causedBy: Envelope<Event>?,
        attribution: Attribution,
        stall: Duration,
    ): PublishResult<T> {
        val preparation = prepare(
            event = event,
            headers = headers,
            context = currentCoroutineContext(),
            cause = causedBy ?: currentEnvelope(),
            principal = principalOf(attribution),
        )
        return when (preparation) {
            is Preparation.Refused -> rejected(preparation.reason)
            is Preparation.Ready -> sent(preparation, stall)
        }
    }

    private suspend fun sent(ready: Preparation.Ready<T>, stall: Duration): PublishResult<T> {
        val refusal = transmitted(ready, stall) ?: return accepted(ready.handoff)
        return rejected(refusal)
    }

    private suspend fun transmitted(ready: Preparation.Ready<T>, stall: Duration): Rejection? {
        var isFinished = false
        try {
            return ready.partition.send(ready.handoff, stall).also { isFinished = true }
        } finally {
            if (!isFinished && !ready.handoff.abandon()) accepted(ready.handoff)
        }
    }

    suspend fun tryPublish(
        event: T,
        headers: EventHeaders,
        causedBy: Envelope<Event>?,
        attribution: Attribution,
    ): PublishResult<T> {
        val preparation = prepare(
            event = event,
            headers = headers,
            context = currentCoroutineContext(),
            cause = causedBy ?: currentEnvelope(),
            principal = principalOf(attribution),
        )
        return offered(preparation)
    }

    fun offer(event: T, headers: EventHeaders, causedBy: Envelope<Event>?, principal: Principal?): PublishResult<T> {
        val preparation = prepare(
            event = event,
            headers = headers,
            context = EmptyCoroutineContext,
            cause = causedBy,
            principal = principal,
        )
        return offered(preparation)
    }

    private fun offered(preparation: Preparation<T>): PublishResult<T> = when (preparation) {
        is Preparation.Refused -> rejected(preparation.reason)
        is Preparation.Ready -> concluded(preparation.handoff, preparation.partition.trySend(preparation.handoff))
    }

    private fun concluded(handoff: PartitionHandoff<T>, result: ChannelResult<Unit>): PublishResult<T> {
        if (result.isSuccess) return accepted(handoff)
        return rejected(Rejection.Closed.takeIf { result.isClosed } ?: Rejection.Overflow)
    }

    private fun prepare(
        event: T,
        headers: EventHeaders,
        context: CoroutineContext,
        cause: Envelope<Event>?,
        principal: Principal?,
    ): Preparation<T> {
        val enriched = enrich(event, headers, context)
        val key = orderingKeyOf(event) ?: return Preparation.Refused(Rejection.InvalidOrderingKey)
        if (enriched.exceeds(environment.limits)) return Preparation.Refused(Rejection.OversizedHeaders)
        val id = environment.identifiers.next()
        val envelope = Envelope(
            id = id,
            route = EnvelopeRoute(topic = runtime.name, orderingKey = key.value),
            lineage = lineage(id, cause),
            headers = enriched,
            payload = event,
            principal = principal,
        )
        return Preparation.Ready(PartitionHandoff(envelope), runtime.partitionFor(envelope))
    }

    private fun accepted(handoff: PartitionHandoff<T>): PublishResult<T> {
        runtime.counters.countPublished()
        environment.observers.observe { BusObservation.Published(handoff.envelope) }
        if (!handoff.accept()) runtime.lostBeforeDispatch(handoff.envelope)
        return PublishResult.Published(handoff.envelope)
    }

    private fun rejected(reason: Rejection): PublishResult<T> {
        runtime.counters.countRejected()
        environment.observers.observe { BusObservation.Rejected(runtime.name, reason) }
        return PublishResult.Rejected(runtime.name, reason)
    }

    private fun enrich(event: T, headers: EventHeaders, context: CoroutineContext): EventHeaders =
        environment.publishInterceptors.fold(headers) { current, interceptor ->
            val publication = Publication(topic = runtime.name, event = event, headers = current, context = context)
            environment.enrich(interceptor, publication)
        }

    private fun orderingKeyOf(event: T): KeyResult? {
        val by = runtime.configuration.orderBy ?: return KeyResult(null)
        val text = contained { by(event) }.getOrNull()
        val isValid = text != null && text.isNotEmpty() && text.length <= environment.limits.maximumOrderingKeyLength
        return text.takeIf { isValid }?.let { valid -> KeyResult(OrderingKey(valid)) }
    }

    private suspend fun principalOf(attribution: Attribution): Principal? = when (attribution) {
        Attribution.Inherited -> currentPrincipal()
        Attribution.Anonymous -> null
        is Attribution.Explicit -> attribution.principal
    }

    private data class KeyResult(val value: OrderingKey?)
}
