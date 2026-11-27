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

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.consume.ConsumerConfiguration
import com.davils.konfetti.core.consume.ConsumerName
import com.davils.konfetti.core.failure.DeadLetter
import com.davils.konfetti.core.failure.FailurePolicy
import com.davils.konfetti.core.failure.HandlingTimeoutException
import com.davils.konfetti.core.internal.bus.BusEnvironment
import com.davils.konfetti.core.internal.bus.DeadLetterOutcome
import com.davils.konfetti.core.internal.bus.DeadLetterSink
import com.davils.konfetti.core.observe.DropReason
import com.davils.kore.core.concurrency.OnceFlag
import com.davils.kore.core.failure.attempted
import com.davils.kore.core.principal.Principal
import com.davils.kore.core.principal.withPrincipal
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration

private const val PROCEEDED_TWICE: String = "A consume interceptor called proceed more than once"

internal class Delivery<T : Event>(
    private val consumer: ConsumerName,
    private val configuration: ConsumerConfiguration<T>,
    private val environment: BusEnvironment,
    private val counters: ConsumerCounters,
    private val deadLetters: DeadLetterSink,
    private val lost: (Envelope<T>, DropReason) -> Unit,
) {
    private val report = DeliveryReport(consumer, counters, environment.observers)
    private val handler = configuration.handling.handler

    suspend fun deliver(envelope: Envelope<T>, reason: () -> DropReason): DeliveryOutcome {
        counters.began()
        var outcome: DeliveryOutcome? = null
        try {
            outcome = settle(envelope)
            return outcome
        } finally {
            counters.ended()
            loseUnsettled(envelope, outcome, reason)
        }
    }

    private fun loseUnsettled(envelope: Envelope<T>, outcome: DeliveryOutcome?, reason: () -> DropReason) {
        if (outcome == null) lost(envelope, reason())
    }

    private suspend fun settle(envelope: Envelope<T>): DeliveryOutcome {
        var attempt = 1
        var outcome = round(envelope, attempt)
        while (outcome == null) {
            attempt += 1
            outcome = round(envelope, attempt)
        }
        return outcome
    }

    private suspend fun round(envelope: Envelope<T>, attempt: Int): DeliveryOutcome? {
        val mark = environment.timeSource.markNow()
        return when (val result = attempt(envelope, attempt)) {
            AttemptResult.Handled -> report.delivered(envelope, attempt, mark.elapsedNow())
            AttemptResult.Filtered -> report.filtered(envelope)
            is AttemptResult.Failed -> recover(envelope, attempt, result.cause)
        }
    }

    private suspend fun recover(envelope: Envelope<T>, attempt: Int, cause: Throwable): DeliveryOutcome? {
        report.failed(envelope, attempt, cause)
        val wait = retryDelay(attempt) ?: return exhausted(envelope, attempt, cause)
        report.retrying(envelope, attempt + 1, wait)
        delay(wait)
        return null
    }

    private suspend fun attempt(envelope: Envelope<T>, number: Int): AttemptResult {
        val result = attempted {
            withContext(HandledEnvelope(envelope, number)) { attributed(envelope.principal) { intercepted(envelope) } }
        }
        return result.getOrElse { failure -> AttemptResult.Failed(failure) }
    }

    private suspend fun attributed(principal: Principal?, block: suspend () -> AttemptResult): AttemptResult {
        if (principal == null) return block()
        return withPrincipal(principal, replacing = true) { block() }
    }

    private suspend fun intercepted(envelope: Envelope<T>): AttemptResult {
        var result: AttemptResult = AttemptResult.Filtered
        proceed(0, envelope) { result = invoke(envelope) }
        return result
    }

    private suspend fun proceed(index: Int, envelope: Envelope<T>, action: suspend () -> Unit) {
        val interceptors = environment.consumeInterceptors
        if (index == interceptors.size) {
            action()
            return
        }
        val once = OnceFlag()
        interceptors[index].intercept(envelope, consumer) {
            check(once.trySet()) { PROCEEDED_TWICE }
            proceed(index + 1, envelope, action)
        }
    }

    private suspend fun invoke(envelope: Envelope<T>): AttemptResult {
        if (!configuration.accepts(envelope)) return AttemptResult.Filtered
        handled(envelope, configuration.recovery.handlingTimeout ?: Duration.INFINITE)
        return AttemptResult.Handled
    }

    private suspend fun handled(envelope: Envelope<T>, timeout: Duration) {
        if (timeout.isInfinite()) return handler(envelope)
        withTimeoutOrNull(timeout) { handler(envelope) } ?: throw HandlingTimeoutException(consumer, timeout)
    }

    private fun retryDelay(attempt: Int): Duration? {
        val policy = (configuration.recovery.onFailure as? FailurePolicy.Retry)?.takeIf { it.attempts > attempt }
            ?: return null
        return policy.backoff.delayBefore(attempt, environment::fraction)
    }

    private suspend fun exhausted(envelope: Envelope<T>, attempts: Int, cause: Throwable): DeliveryOutcome =
        when (configuration.recovery.onFailure.terminal) {
            FailurePolicy.Skip -> {
                report.skipped(envelope, attempts)
            }

            FailurePolicy.DeadLetter -> {
                deadLetter(envelope, attempts, cause)
            }

            FailurePolicy.Stop -> {
                lost(envelope, DropReason.Stopped)
                DeliveryOutcome.Stop(cause)
            }
        }

    private suspend fun deadLetter(envelope: Envelope<T>, attempts: Int, cause: Throwable): DeliveryOutcome {
        val letter = DeadLetter(original = envelope, consumer = consumer, attempts = attempts, cause = cause)
        when (deadLetters.accept(letter, envelope)) {
            DeadLetterOutcome.Accepted -> return report.deadLettered(envelope, attempts)
            DeadLetterOutcome.Closed -> lost(envelope, DropReason.Closed)
            DeadLetterOutcome.Overflow -> lost(envelope, DropReason.DeadLetterOverflow)
        }
        return DeliveryOutcome.Continue
    }
}
