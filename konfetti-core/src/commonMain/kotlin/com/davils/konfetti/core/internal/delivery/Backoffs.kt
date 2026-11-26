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

@file:OptIn(DavilsInternal::class)

package com.davils.konfetti.core.internal.delivery

import com.davils.konfetti.core.failure.Backoff
import com.davils.konfetti.core.failure.FailurePolicy
import com.davils.kore.core.annotation.DavilsInternal
import com.davils.kore.core.backoff.fullJitter
import com.davils.kore.core.backoff.grownDelay
import kotlin.time.Duration

internal val FailurePolicy.terminal: FailurePolicy.Terminal
    get() = when (this) {
        is FailurePolicy.Retry -> then
        is FailurePolicy.Terminal -> this
    }

internal fun Backoff.delayBefore(retry: Int, fraction: () -> Double): Duration = when (this) {
    is Backoff.Fixed -> delay
    is Backoff.Exponential -> grown(this, retry, fraction)
}

internal val Backoff.longestDelay: Duration
    get() = when (this) {
        is Backoff.Fixed -> delay
        is Backoff.Exponential -> max
    }

private fun grown(backoff: Backoff.Exponential, retry: Int, fraction: () -> Double): Duration {
    val capped = grownDelay(
        initial = backoff.initial,
        multiplier = backoff.factor,
        step = retry - 1,
        maximum = backoff.max,
    )
    if (!backoff.hasJitter) return capped
    return fullJitter(delay = capped, fraction = fraction())
}
