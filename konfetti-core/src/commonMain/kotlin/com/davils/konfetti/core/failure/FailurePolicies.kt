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

package com.davils.konfetti.core.failure

import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private const val DEFAULT_FACTOR: Double = 2.0
private val defaultInitialDelay: Duration = 100.milliseconds
private val defaultMaximumDelay: Duration = 10.seconds

/**
 * Makes a policy that tries an envelope up to [attempts] times and then decides with [then].
 *
 * The policy is validated when the consumer is attached.
 *
 * @param attempts the attempts in all, the first one included.
 * @param backoff the wait before each retry; exponential from 100 milliseconds to 10 seconds by default.
 * @param then what to do when every attempt failed; [FailurePolicy.DeadLetter] by default.
 * @return the policy.
 * @since 1.0.0
 */
public fun retry(
    attempts: Int,
    backoff: Backoff = exponential(initial = defaultInitialDelay, max = defaultMaximumDelay),
    then: FailurePolicy.Terminal = FailurePolicy.DeadLetter,
): FailurePolicy.Retry = FailurePolicy.Retry(attempts = attempts, backoff = backoff, then = then)

/**
 * Makes a backoff with the same [delay] before every retry.
 *
 * @param delay the delay.
 * @return the backoff.
 * @since 1.0.0
 */
public fun fixed(delay: Duration): Backoff.Fixed = Backoff.Fixed(delay)

/**
 * Makes a backoff that starts at [initial] and grows by [factor] up to [max].
 *
 * @param initial the delay before the first retry.
 * @param max the longest delay.
 * @param factor what each delay is multiplied by for the next retry; 2 by default.
 * @param jitter whether each delay is a random delay between zero and the grown delay.
 * @return the backoff.
 * @since 1.0.0
 */
public fun exponential(
    initial: Duration,
    max: Duration,
    factor: Double = DEFAULT_FACTOR,
    jitter: Boolean = false,
): Backoff.Exponential = Backoff.Exponential(initial = initial, max = max, factor = factor, hasJitter = jitter)
