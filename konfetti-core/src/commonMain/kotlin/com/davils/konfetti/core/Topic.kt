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

import com.davils.konfetti.core.consume.Consumer
import com.davils.konfetti.core.consume.ConsumerBuilder
import com.davils.konfetti.core.consume.ConsumerConfiguration
import com.davils.konfetti.core.consume.Consumption
import com.davils.konfetti.core.consume.StartPosition
import com.davils.konfetti.core.failure.FailurePolicy
import com.davils.konfetti.core.header.EventHeaders
import com.davils.konfetti.core.internal.bus.DeadLetterOutcome
import com.davils.konfetti.core.internal.delivery.ConsumerRuntime
import com.davils.konfetti.core.internal.delivery.ConsumerTarget
import com.davils.konfetti.core.internal.displayName
import com.davils.konfetti.core.internal.partition.TopicRuntime
import com.davils.konfetti.core.internal.publish.Publisher
import com.davils.konfetti.core.lifecycle.BusState
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.core.observe.TopicMetrics
import com.davils.konfetti.core.publish.Attribution
import com.davils.konfetti.core.publish.PublishResult
import com.davils.konfetti.core.publish.Rejection
import com.davils.konfetti.core.topic.TopicName
import com.davils.kore.core.principal.Principal
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.incrementAndFetch
import kotlin.reflect.KClass
import kotlin.time.Duration

private const val FLOW_PREFIX: String = "konfetti.flow-"

/**
 * A topic of a bus: where events of one type are published and from where consumers receive them.
 *
 * A topic splits its envelopes over its partitions; envelopes of one partition reach every consumer in the order they
 * were accepted. Every consumer receives every envelope accepted after it was attached, and accounts for each exactly
 * once: it delivers, filters, skips, dead-letters or drops it.
 *
 * A topic is safe to use from any number of coroutines and threads.
 *
 * @since 1.0.0
 */
@OptIn(ExperimentalAtomicApi::class)
public class Topic<T : Event> internal constructor(internal val runtime: TopicRuntime<T>) {
    private val publisher = Publisher(runtime)
    private val flows = AtomicInt(0)

    /**
     * The name of the topic.
     *
     * @since 1.0.0
     */
    public val name: TopicName get() = runtime.name

    /**
     * The class of the events of the topic.
     *
     * @since 1.0.0
     */
    public val eventType: KClass<T> get() = runtime.configuration.eventType

    /**
     * The partitions of the topic.
     *
     * @since 1.0.0
     */
    public val partitionCount: Int get() = runtime.configuration.partitions

    /**
     * The state of the topic.
     *
     * @since 1.0.0
     */
    public val state: BusState get() = runtime.state

    /**
     * Whether a publication would reach anyone now: the topic runs and has a consumer or retains envelopes.
     *
     * A library that publishes events no one may want can skip the work of building them while this is false.
     *
     * @since 1.0.0
     */
    public val hasConsumers: Boolean get() = runtime.hasConsumers

    /**
     * The counts of the topic now.
     *
     * @since 1.0.0
     */
    public val metrics: TopicMetrics get() = runtime.metrics()

    /**
     * Publishes [event], waiting while its partition is full.
     *
     * The publish interceptors of the bus enrich the headers first; the ordering key of the topic picks the partition.
     * A publication from inside a handler takes the envelope being handled as its cause unless [causedBy] names
     * another.
     *
     * @param event the event.
     * @param headers the headers of the envelope; none by default.
     * @param causedBy the envelope that caused this one, or null for the envelope being handled, if any.
     * @param attribution whom the publication is attributed to; the principal of the calling coroutine by default.
     * @return the envelope, or a rejection with [Rejection.Closed], [Rejection.InvalidOrderingKey] or
     * [Rejection.OversizedHeaders].
     * @since 1.0.0
     */
    public suspend fun publish(
        event: T,
        headers: EventHeaders = EventHeaders.EMPTY,
        causedBy: Envelope<Event>? = null,
        attribution: Attribution = Attribution.Inherited,
    ): PublishResult<T> = publisher.publish(
        event = event,
        headers = headers,
        causedBy = causedBy,
        attribution = attribution,
        stall = Duration.INFINITE,
    )

    /**
     * Publishes [event] only if its partition has room now, and never waits.
     *
     * It behaves as [publish] in everything else: interceptors, cause and attribution.
     *
     * @param event the event.
     * @param headers the headers of the envelope; none by default.
     * @param causedBy the envelope that caused this one, or null for the envelope being handled, if any.
     * @param attribution whom the publication is attributed to; the principal of the calling coroutine by default.
     * @return the envelope, or a rejection, with [Rejection.Overflow] if the partition is full.
     * @since 1.0.0
     */
    public suspend fun tryPublish(
        event: T,
        headers: EventHeaders = EventHeaders.EMPTY,
        causedBy: Envelope<Event>? = null,
        attribution: Attribution = Attribution.Inherited,
    ): PublishResult<T> = publisher.tryPublish(
        event = event,
        headers = headers,
        causedBy = causedBy,
        attribution = attribution,
    )

    /**
     * Publishes [event] from outside a coroutine if its partition has room now, and never waits.
     *
     * Without a coroutine there is nothing to inherit: the cause is [causedBy] alone, the principal is [principal]
     * alone, and the interceptors see the empty coroutine context.
     *
     * @param event the event.
     * @param headers the headers of the envelope; none by default.
     * @param causedBy the envelope that caused this one, or null.
     * @param principal the principal the publication is attributed to, or null for none.
     * @return the envelope, or a rejection, with [Rejection.Overflow] if the partition is full.
     * @since 1.0.0
     */
    public fun offer(
        event: T,
        headers: EventHeaders = EventHeaders.EMPTY,
        causedBy: Envelope<Event>? = null,
        principal: Principal? = null,
    ): PublishResult<T> = publisher.offer(event = event, headers = headers, causedBy = causedBy, principal = principal)

    /**
     * Attaches a consumer named [name] to the topic.
     *
     * The consumer starts at once. When [owner] completes, the consumer is detached, so a consumer can live exactly as
     * long as the component that owns it.
     *
     * @param name the name of the consumer, a [com.davils.konfetti.core.consume.ConsumerName] unique on the topic.
     * @param owner the scope whose end detaches the consumer, or null to detach it only by hand or with the bus.
     * @param block declares the handler and the settings of the consumer.
     * @return the attached consumer, or why none was attached.
     * @since 1.0.0
     */
    public fun consume(name: String, owner: CoroutineScope? = null, block: ConsumerBuilder<T>.() -> Unit): Consumption {
        val target = ConsumerTarget(
            partitions = partitionCount,
            isDeadLetterTopic = runtime.isDeadLetterTopic,
            limits = runtime.environment.limits,
        )
        val builder = ConsumerBuilder<T>(name = name, target = target, defaults = runtime.environment.consumerDefaults)
        builder.block()
        val report = builder.validate()
        if (!report.isValid) return Consumption.Invalid(report)
        return attach(builder.build(), owner)
    }

    /**
     * Returns the envelopes of the topic as a flow.
     *
     * Each collection attaches its own consumer named `konfetti.flow-n`, which skips what its collector fails and is
     * detached when the collection ends. On a topic that no longer runs the flow ends at once; it fails with an
     * [IllegalStateException] if the consumer cannot be attached otherwise, such as when the topic holds its limit of
     * consumers.
     *
     * @param start where the flow starts; the envelopes published from now on by default.
     * @return a flow of envelopes that ends when the topic closes.
     * @since 1.0.0
     */
    public fun events(start: StartPosition = StartPosition.Latest): Flow<Envelope<T>> = channelFlow {
        val position = start
        val consumption = consume("$FLOW_PREFIX${flows.incrementAndFetch()}") {
            this.start = position
            onFailure = FailurePolicy.Skip
            handle { envelope -> send(envelope) }
        }
        consumption.consumerOrNull()?.let { consumer -> awaitClose { consumer.detach() } }
    }

    /**
     * Shows the topic as `Topic(name: Type)`.
     *
     * @return the topic as text.
     * @since 1.0.0
     */
    override fun toString(): String = "Topic($name: ${eventType.displayName})"

    internal suspend fun store(letter: T, cause: Envelope<Event>, stall: Duration): DeadLetterOutcome {
        val attribution = Attribution.forPrincipal(cause.principal)
        return when (
            val result = publisher.publish(
                event = letter,
                headers = EventHeaders.EMPTY,
                causedBy = cause,
                attribution = attribution,
                stall = stall,
            )
        ) {
            is PublishResult.Published -> DeadLetterOutcome.Accepted
            is PublishResult.Rejected -> outcomeOf(result.reason)
        }
    }

    private fun outcomeOf(reason: Rejection): DeadLetterOutcome {
        if (reason == Rejection.Overflow) return DeadLetterOutcome.Overflow
        return DeadLetterOutcome.Closed
    }

    private fun attach(configuration: ConsumerConfiguration<T>, owner: CoroutineScope?): Consumption {
        val consumer = ConsumerRuntime(
            configuration = configuration,
            topic = name,
            environment = runtime.environment,
            deadLetters = runtime.deadLetters,
            parent = runtime.scope,
            release = runtime.registry::release,
        )
        val refusal = runtime.attach(consumer)
        if (refusal != null) {
            consumer.closeWith(DropReason.Closed)
            return refusal
        }
        consumer.start()
        consumer.watch(owner)
        return Consumption.Attached(Consumer(consumer))
    }
}

private fun Consumption.consumerOrNull(): Consumer? {
    if (this is Consumption.Closed) return null
    return getOrThrow()
}
