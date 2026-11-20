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

package com.davils.konfetti.core.consume

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.failure.FailurePolicy
import com.davils.konfetti.core.internal.delivery.ConsumerHandling
import com.davils.konfetti.core.internal.delivery.ConsumerIntake
import com.davils.konfetti.core.internal.delivery.ConsumerRecovery
import com.davils.konfetti.core.internal.delivery.ConsumerSettings
import com.davils.konfetti.core.internal.delivery.ConsumerTarget
import com.davils.kore.core.annotation.DavilsDsl
import com.davils.kore.core.dsl.ValidatingBuilder
import kotlin.time.Duration

/**
 * Builds a consumer of one topic; every property starts from the consumer defaults of the bus.
 *
 * A consumer declares exactly one handler with [handle] or [handleOnly], and any number of predicates with [where].
 *
 * @since 1.0.0
 */
@DavilsDsl
public class ConsumerBuilder<T : Event> internal constructor(
    private val name: String,
    private val target: ConsumerTarget,
    defaults: ConsumerSettings,
) : ValidatingBuilder<ConsumerConfiguration<T>>() {
    private val predicates = mutableListOf<(Envelope<T>) -> Boolean>()
    private var handler: suspend (Envelope<T>) -> Unit = {}
    private var handlers: Int = 0

    override val subject: String get() = "consumer"

    /**
     * The workers of the consumer, between 1 and the partitions of the topic: 1 by default.
     *
     * Each partition belongs to one worker, so envelopes of one partition are handled one after another in their
     * order, and envelopes of different partitions may be handled at the same time.
     *
     * @since 1.0.0
     */
    public var concurrency: Int = 1

    /**
     * The envelopes the mailbox of each worker holds, between 1 and
     * [com.davils.konfetti.core.limit.BusLimits.maximumMailboxCapacity].
     *
     * @since 1.0.0
     */
    public var mailboxCapacity: Int = defaults.mailboxCapacity

    /**
     * Where the consumer starts.
     *
     * @since 1.0.0
     */
    public var start: StartPosition = defaults.start

    /**
     * What the consumer does when a mailbox is full.
     *
     * @since 1.0.0
     */
    public var overflow: OverflowPolicy = defaults.overflow

    /**
     * How long a full mailbox may hold up its partition under [OverflowPolicy.Backpressure], positive;
     * [Duration.INFINITE] waits without limit.
     *
     * @since 1.0.0
     */
    public var maximumStall: Duration = defaults.maximumStall

    /**
     * How long one attempt of the handler may take, positive and at most
     * [com.davils.konfetti.core.limit.BusLimits.maximumHandlingTimeout], or null for no limit. An attempt that takes
     * longer is cancelled and fails with a [com.davils.konfetti.core.failure.HandlingTimeoutException].
     *
     * @since 1.0.0
     */
    public var handlingTimeout: Duration? = defaults.handlingTimeout

    /**
     * What the consumer does with an envelope it failed.
     *
     * @since 1.0.0
     */
    public var onFailure: FailurePolicy = defaults.onFailure

    /**
     * Adds a predicate an envelope must pass to reach the handler; an envelope that fails a predicate is counted as
     * filtered.
     *
     * Predicates run on the worker for every envelope and must be fast; a predicate that throws fails the attempt.
     *
     * @param predicate the predicate.
     * @since 1.0.0
     */
    public fun where(predicate: (Envelope<T>) -> Boolean) {
        predicates += predicate
    }

    /**
     * Declares the handler of the consumer.
     *
     * The handler runs on a worker of the consumer with the principal of the envelope established and with
     * [com.davils.konfetti.core.currentEnvelope] returning the envelope. A handler that throws fails the attempt.
     *
     * @param handler the handler.
     * @since 1.0.0
     */
    public fun handle(handler: suspend (Envelope<T>) -> Unit) {
        this.handler = handler
        handlers += 1
    }

    /**
     * Declares a handler of the events of type [R] only; other events are filtered.
     *
     * @param R the type of the events to handle.
     * @param handler the handler.
     * @since 1.0.0
     */
    public inline fun <reified R : T> handleOnly(noinline handler: suspend (Envelope<R>) -> Unit) {
        where { envelope -> envelope.payload is R }
        @Suppress("UNCHECKED_CAST")
        handle { envelope -> handler(envelope as Envelope<R>) }
    }

    override fun assemble(): ConsumerConfiguration<T> = ConsumerConfiguration(
        name = name,
        target = target,
        intake = ConsumerIntake(
            start = start,
            concurrency = concurrency,
            mailboxCapacity = mailboxCapacity,
            overflow = overflow,
            maximumStall = maximumStall,
        ),
        recovery = ConsumerRecovery(handlingTimeout = handlingTimeout, onFailure = onFailure),
        handling = ConsumerHandling(predicates = predicates.toList(), handler = handler, handlers = handlers),
    )
}
