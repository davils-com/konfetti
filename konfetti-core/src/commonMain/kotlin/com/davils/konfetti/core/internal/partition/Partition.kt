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

internal class Partition<T : Event>(capacity: Int, lost: (Envelope<T>) -> Unit) {
    private val channel = Channel<PartitionHandoff<T>>(capacity) { handoff ->
        if (handoff.undeliver()) lost(handoff.envelope)
    }

    suspend fun send(handoff: PartitionHandoff<T>, stall: Duration): Rejection? {
        if (stall.isInfinite()) return refusalOf(attempted { channel.send(handoff) })
        val sent = withTimeoutOrNull(stall) { attempted { channel.send(handoff) } }
        return sent?.let(::refusalOf) ?: Rejection.Overflow.takeIf { sent == null }
    }

    fun trySend(handoff: PartitionHandoff<T>): ChannelResult<Unit> = channel.trySend(handoff)

    fun start(scope: CoroutineScope, dispatch: suspend (Envelope<T>) -> Unit): Job =
        scope.launch(start = CoroutineStart.UNDISPATCHED) {
            for (handoff in channel) dispatch(handoff.envelope)
        }

    fun close() {
        channel.close()
    }

    fun cancel() {
        channel.cancel()
    }

    private fun refusalOf(sent: Result<Unit>): Rejection? = Rejection.Closed.takeIf { sent.isFailure }
}
