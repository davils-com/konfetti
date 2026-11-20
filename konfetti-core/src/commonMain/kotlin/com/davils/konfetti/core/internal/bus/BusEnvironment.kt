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

package com.davils.konfetti.core.internal.bus

import com.davils.konfetti.core.consume.ConsumeInterceptor
import com.davils.konfetti.core.header.EventHeaders
import com.davils.konfetti.core.internal.delivery.ConsumerSettings
import com.davils.konfetti.core.internal.partition.SipKey
import com.davils.konfetti.core.internal.partition.TopicSettings
import com.davils.konfetti.core.internal.platform.guard
import com.davils.konfetti.core.internal.platform.stripedCounter
import com.davils.konfetti.core.limit.BusLimits
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.core.publish.Publication
import com.davils.konfetti.core.publish.PublishInterceptor
import com.davils.kore.core.failure.contained
import com.davils.kore.core.identity.UuidV7
import com.davils.kore.core.identity.generator.UuidGenerator
import kotlin.time.Duration
import kotlin.time.TimeSource

private const val FRACTION_SHIFT: Int = 11
private const val FRACTION_SCALE: Double = 1.0 / (1L shl 53)

internal class BusEnvironment(
    private val sources: BusSources,
    hooks: BusHooks,
    defaults: BusDefaults,
    val limits: BusLimits,
    val deadLetterStall: Duration,
) {
    private val entropy = guard()
    private val losses = stripedCounter()
    private val interceptorFailures = stripedCounter()

    val observers: Observers = Observers(hooks.observers)

    val identifiers: UuidGenerator<UuidV7> get() = sources.identifiers

    val timeSource: TimeSource.WithComparableMarks get() = sources.timeSource

    val publishInterceptors: List<PublishInterceptor> = hooks.publishInterceptors

    val consumeInterceptors: List<ConsumeInterceptor> = hooks.consumeInterceptors

    val topicDefaults: TopicSettings = defaults.topic

    val consumerDefaults: ConsumerSettings = defaults.consumer

    val partitionKey: SipKey = SipKey(secretBits(), secretBits())

    val lost: Long get() = losses.sum

    val interceptorFailureCount: Long get() = interceptorFailures.sum

    fun fraction(): Double {
        val bits = entropy.locked { contained { sources.entropy.nextLong() } }
        return bits.map { drawn -> (drawn ushr FRACTION_SHIFT) * FRACTION_SCALE }.getOrDefault(1.0)
    }

    fun recordLoss(reason: DropReason) {
        if (reason == DropReason.Closed || reason == DropReason.Abandoned) losses.increment()
    }

    fun enrich(interceptor: PublishInterceptor, publication: Publication): EventHeaders {
        val enriched = contained { interceptor.enrich(publication) }
        return enriched.getOrElse {
            interceptorFailures.increment()
            publication.headers
        }
    }

    private fun secretBits(): Long {
        val drawn = entropy.locked { contained { sources.entropy.nextLong() } }
        return drawn.getOrElse { SecureEntropy.nextLong() }
    }
}
