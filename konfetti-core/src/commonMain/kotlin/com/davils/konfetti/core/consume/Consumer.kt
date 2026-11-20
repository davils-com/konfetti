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

import com.davils.konfetti.core.internal.delivery.ConsumerRuntime
import com.davils.konfetti.core.lifecycle.DrainResult
import com.davils.konfetti.core.observe.ConsumerMetrics
import com.davils.konfetti.core.topic.TopicName
import kotlinx.coroutines.flow.StateFlow
import kotlin.time.Duration

/**
 * A consumer attached to a topic.
 *
 * @since 1.0.0
 */
public class Consumer internal constructor(private val runtime: ConsumerRuntime<*>) {
    /**
     * The name of the consumer.
     *
     * @since 1.0.0
     */
    public val name: ConsumerName get() = runtime.name

    /**
     * The topic of the consumer.
     *
     * @since 1.0.0
     */
    public val topic: TopicName get() = runtime.topic

    /**
     * The state of the consumer.
     *
     * @since 1.0.0
     */
    public val state: StateFlow<ConsumerState> get() = runtime.state

    /**
     * The counts of the consumer now.
     *
     * @since 1.0.0
     */
    public val metrics: ConsumerMetrics get() = runtime.metrics()

    /**
     * Detaches the consumer at once: what it received and has not settled is dropped with
     * [com.davils.konfetti.core.observe.DropReason.Detached], and an attempt in progress is cancelled. Detaching a
     * detached or stopped consumer does nothing.
     *
     * @since 1.0.0
     */
    public fun detach() {
        runtime.detach()
    }

    /**
     * Stops the consumer from receiving and waits until it settled what it received, then detaches it.
     *
     * @param timeout how long to wait; what is still unsettled then is dropped with
     * [com.davils.konfetti.core.observe.DropReason.Abandoned].
     * @return [DrainResult.Completed], [DrainResult.TimedOut] with the abandoned envelopes, or
     * [DrainResult.AlreadyClosed] if the consumer no longer ran.
     * @since 1.0.0
     */
    public suspend fun drain(timeout: Duration): DrainResult = runtime.drain(timeout)

    /**
     * Shows the consumer as `Consumer(topic/name)`.
     *
     * @return the consumer as text.
     * @since 1.0.0
     */
    override fun toString(): String = "Consumer($topic/$name)"
}
