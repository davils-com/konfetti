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
import com.davils.konfetti.core.consume.OverflowPolicy
import com.davils.konfetti.core.observe.DropReason
import com.davils.kore.core.failure.attempted
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration

private const val EVICTION_ATTEMPTS: Int = 16

internal class Mailbox<T : Event>(
    capacity: Int,
    private val overflow: OverflowPolicy,
    private val maximumStall: Duration,
    private val lost: (Envelope<T>, DropReason) -> Unit,
    private val reason: () -> DropReason,
) {
    private val channel = Channel<MailboxHandoff<T>>(capacity) { handoff ->
        if (!handoff.defer()) lose(handoff, reason())
    }

    suspend fun put(envelope: Envelope<T>) {
        val handoff = MailboxHandoff(envelope)
        settle(handoff, placed(handoff))
    }

    private suspend fun placed(handoff: MailboxHandoff<T>): DropReason? {
        var isFinished = false
        try {
            return place(handoff).also { isFinished = true }
        } finally {
            if (!isFinished) lose(handoff, reason())
        }
    }

    private fun settle(handoff: MailboxHandoff<T>, refusal: DropReason?) {
        if (refusal != null) return lose(handoff, refusal)
        if (!handoff.confirm()) lose(handoff, reason())
    }

    suspend fun consume(block: suspend (Envelope<T>) -> Unit) {
        for (handoff in channel) {
            if (handoff.claim()) block(handoff.envelope)
        }
    }

    fun close() {
        channel.close()
    }

    fun cancel() {
        channel.cancel()
    }

    private suspend fun place(handoff: MailboxHandoff<T>): DropReason? = when (overflow) {
        OverflowPolicy.Backpressure -> send(handoff)
        OverflowPolicy.DropOldest -> dropOldest(handoff)
        OverflowPolicy.DropLatest -> dropLatest(handoff)
    }

    private suspend fun send(handoff: MailboxHandoff<T>): DropReason? {
        if (maximumStall.isInfinite()) return refusalOf(attempted { channel.send(handoff) })
        val sent = withTimeoutOrNull(maximumStall) { attempted { channel.send(handoff) } }
        return sent?.let(::refusalOf) ?: DropReason.Stalled.takeIf { sent == null }
    }

    private fun dropLatest(handoff: MailboxHandoff<T>): DropReason? {
        val result = channel.trySend(handoff)
        if (result.isSuccess) return null
        return reason().takeIf { result.isClosed } ?: DropReason.Overflow
    }

    private fun dropOldest(handoff: MailboxHandoff<T>): DropReason? {
        repeat(EVICTION_ATTEMPTS) {
            if (channel.trySend(handoff).isSuccess) return null
            evictOldest()
        }
        return dropLatest(handoff)
    }

    private fun evictOldest() {
        val evicted = channel.tryReceive().getOrNull() ?: return
        lose(evicted, DropReason.Overflow)
    }

    private fun refusalOf(sent: Result<Unit>): DropReason? {
        if (sent.isSuccess) return null
        return reason()
    }

    private fun lose(handoff: MailboxHandoff<T>, reason: DropReason) {
        if (handoff.claim()) lost(handoff.envelope, reason)
    }
}
