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
import com.davils.konfetti.core.error.EventViolationCodes
import com.davils.konfetti.core.failure.FailurePolicy
import com.davils.konfetti.core.internal.delivery.ConsumerHandling
import com.davils.konfetti.core.internal.delivery.ConsumerIntake
import com.davils.konfetti.core.internal.delivery.ConsumerRecovery
import com.davils.konfetti.core.internal.delivery.ConsumerTarget
import com.davils.konfetti.core.internal.delivery.longestDelay
import com.davils.konfetti.core.internal.delivery.terminal
import com.davils.konfetti.core.limit.BusLimits
import com.davils.kore.core.validation.Validatable
import com.davils.kore.core.validation.ValidationScope
import com.davils.kore.core.validation.violation.ViolationCode

/**
 * The validated settings of one consumer, built by a [ConsumerBuilder].
 *
 * The name is a [ConsumerName]; the concurrency lies between 1 and the partitions of the topic; the mailbox, the
 * handling timeout, the attempts and the delays of a retry stay within the limits of the bus; the longest stall is
 * positive; exactly one handler is declared; and a consumer of the dead letters does not send its failures back to the
 * dead letters.
 *
 * @since 1.0.0
 */
public class ConsumerConfiguration<T : Event> internal constructor(
    internal val name: String,
    internal val target: ConsumerTarget,
    internal val intake: ConsumerIntake,
    internal val recovery: ConsumerRecovery,
    internal val handling: ConsumerHandling<T>,
) : Validatable {
    internal val consumerName: ConsumerName get() = ConsumerName(name)

    internal fun accepts(envelope: Envelope<T>): Boolean = handling.predicates.all { predicate -> predicate(envelope) }

    /**
     * Checks the settings against the topic and the limits of the bus.
     *
     * @receiver the validation that collects the violations.
     * @since 1.0.0
     */
    override fun ValidationScope.validate() {
        if (!consumerNames.matches(name)) {
            fail(::name, "is no consumer name such as inventory.indexer", EventViolationCodes.INVALID_NAME)
        }
        validateIntake()
        validateTimeout()
        validateHandlers()
        validateFailure()
    }

    private fun ValidationScope.validateIntake() {
        if (intake.concurrency !in 1..target.partitions) {
            fail(
                property = intake::concurrency,
                message = "must be between 1 and the ${target.partitions} partitions of the topic",
                code = EventViolationCodes.INVALID_CONCURRENCY,
            )
        }
        val mailboxes = target.limits.maximumMailboxCapacity
        if (intake.mailboxCapacity !in 1..mailboxes) {
            fail(intake::mailboxCapacity, "must be between 1 and $mailboxes", EventViolationCodes.INVALID_MAILBOX)
        }
        if (intake.maximumStall.isPositive()) return
        fail(intake::maximumStall, "must be positive", EventViolationCodes.INVALID_STALL)
    }

    private fun ValidationScope.validateTimeout() {
        val timeout = recovery.handlingTimeout ?: return
        val longest = target.limits.maximumHandlingTimeout
        if (timeout.isPositive() && timeout <= longest) return
        fail(recovery::handlingTimeout, "must be positive and at most $longest", EventViolationCodes.INVALID_TIMEOUT)
    }

    private fun ValidationScope.validateHandlers() {
        val declared = handling.handlers
        if (declared == 0) fail(handling::handler, "must be declared", EventViolationCodes.MISSING_HANDLER)
        if (declared <= 1) return
        fail(handling::handler, "is declared $declared times, but a consumer has one", ViolationCode.DUPLICATE)
    }

    private fun ValidationScope.validateFailure() {
        val policy = recovery.onFailure
        if (policy is FailurePolicy.Retry) within(recovery::onFailure) { validateRetry(policy) }
        if (!target.isDeadLetterTopic || policy.terminal != FailurePolicy.DeadLetter) return
        fail(
            property = recovery::onFailure,
            message = "must not send the dead letters of the bus back to the dead letters",
            code = EventViolationCodes.DEAD_LETTER_LOOP,
        )
    }

    private fun ValidationScope.validateRetry(policy: FailurePolicy.Retry) {
        with(policy) { validate() }
        val limits = target.limits
        if (policy.attempts > limits.maximumAttempts && policy.attempts <= BusLimits.HARD_ATTEMPTS) {
            fail(policy::attempts, "must be at most ${limits.maximumAttempts}", EventViolationCodes.INVALID_RETRY)
        }
        val longest = policy.backoff.longestDelay
        if (longest <= limits.maximumRetryDelay || longest > BusLimits.HARD_RETRY_DELAY) return
        fail(policy::backoff, "must wait at most ${limits.maximumRetryDelay}", EventViolationCodes.INVALID_BACKOFF)
    }
}
