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

package com.davils.konfetti.core.internal.partition

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.internal.platform.guard
import com.davils.konfetti.core.publish.Rejection
import com.davils.kore.core.failure.attempted
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration

internal class Partition<T : Event>(capacity: Int, private val lost: (Envelope<T>) -> Unit) {
    private val guard = guard()
    private val waiting = mutableSetOf<PartitionHandoff<T>>()
    private val channel = Channel<PartitionHandoff<T>>(capacity) { handoff -> undeliver(handoff) }

    suspend fun send(handoff: PartitionHandoff<T>, stall: Duration): Rejection? {
        guard.locked { waiting += handoff }
        if (stall.isInfinite()) return refusalOf(handoff, attempted { channel.send(handoff) })
        return sendWithin(handoff, stall)
    }

    fun trySend(handoff: PartitionHandoff<T>): ChannelResult<Unit> {
        guard.locked { waiting += handoff }
        val result = channel.trySend(handoff)
        if (result.isFailure) forget(handoff)
        return result
    }

    fun start(scope: CoroutineScope, dispatch: suspend (Envelope<T>) -> Unit): Job =
        scope.launch(start = CoroutineStart.UNDISPATCHED) {
            for (handoff in channel) {
                forget(handoff)
                if (handoff.take()) dispatch(handoff.envelope)
            }
        }

    fun close() {
        channel.close()
    }

    fun cancel() {
        channel.cancel()
        val stranded = guard.locked { waiting.toList() }
        stranded.forEach(::undeliver)
    }

    private suspend fun sendWithin(handoff: PartitionHandoff<T>, stall: Duration): Rejection? {
        val sent = withTimeoutOrNull(stall) { attempted { channel.send(handoff) } }
            ?: return refused(handoff, Rejection.Overflow)
        return refusalOf(handoff, sent)
    }

    private fun refusalOf(handoff: PartitionHandoff<T>, sent: Result<Unit>): Rejection? {
        if (sent.isSuccess) return null
        return refused(handoff, Rejection.Closed)
    }

    private fun refused(handoff: PartitionHandoff<T>, rejection: Rejection): Rejection? {
        forget(handoff)
        return rejection.takeIf { handoff.abandon() }
    }

    private fun undeliver(handoff: PartitionHandoff<T>) {
        forget(handoff)
        if (handoff.undeliver()) lost(handoff.envelope)
    }

    private fun forget(handoff: PartitionHandoff<T>) {
        guard.locked { waiting -= handoff }
    }
}
