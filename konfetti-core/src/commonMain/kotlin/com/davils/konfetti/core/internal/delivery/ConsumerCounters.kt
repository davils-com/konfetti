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

package com.davils.konfetti.core.internal.delivery

import com.davils.konfetti.core.internal.platform.stripedCounter
import com.davils.konfetti.core.observe.ConsumerMetrics

internal class ConsumerCounters {
    private val received = stripedCounter()
    private val delivered = stripedCounter()
    private val filtered = stripedCounter()
    private val failed = stripedCounter()
    private val retried = stripedCounter()
    private val skipped = stripedCounter()
    private val deadLettered = stripedCounter()
    private val dropped = stripedCounter()
    private val inFlight = stripedCounter()

    val droppedCount: Long get() = dropped.sum

    fun countReceived() {
        received.increment()
    }

    fun countDelivered() {
        delivered.increment()
    }

    fun countFiltered() {
        filtered.increment()
    }

    fun countFailed() {
        failed.increment()
    }

    fun countRetried() {
        retried.increment()
    }

    fun countSkipped() {
        skipped.increment()
    }

    fun countDeadLettered() {
        deadLettered.increment()
    }

    fun countDropped() {
        dropped.increment()
    }

    fun began() {
        inFlight.increment()
    }

    fun ended() {
        inFlight.decrement()
    }

    fun metrics(): ConsumerMetrics {
        val handling = inFlight.sum
        val receivedCount = received.sum
        val deliveredCount = delivered.sum
        val filteredCount = filtered.sum
        val skippedCount = skipped.sum
        val deadLetteredCount = deadLettered.sum
        val droppedTotal = dropped.sum
        val settled = deliveredCount + filteredCount + skippedCount + deadLetteredCount + droppedTotal
        return ConsumerMetrics(
            received = receivedCount,
            delivered = deliveredCount,
            filtered = filteredCount,
            failed = failed.sum,
            retried = retried.sum,
            skipped = skippedCount,
            deadLettered = deadLetteredCount,
            dropped = droppedTotal,
            inFlight = handling,
            queued = (receivedCount - settled - handling).coerceAtLeast(0),
        )
    }
}
