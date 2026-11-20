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

import com.davils.konfetti.core.internal.platform.guard
import com.davils.konfetti.core.lifecycle.BusState
import com.davils.konfetti.core.lifecycle.CloseReport
import com.davils.konfetti.core.lifecycle.DrainResult
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.DropReason
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.job
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.time.Duration

@OptIn(ExperimentalAtomicApi::class)
internal class BusLifecycle(
    private val environment: BusEnvironment,
    private val parent: CoroutineScope,
    private val directory: TopicDirectory,
) {
    private val lock = guard()
    private val mutableState = MutableStateFlow(BusState.Running)
    private val termination = CompletableDeferred<DrainResult>()
    private val scopeWatch = AtomicReference<DisposableHandle?>(null)

    val state: StateFlow<BusState> = mutableState.asStateFlow()

    fun watch(scope: CoroutineScope) {
        val job = scope.coroutineContext[Job] ?: return
        scopeWatch.store(job.invokeOnCompletion { closeNow() })
    }

    fun <R : Any> ifRunning(block: () -> R): R? = lock.locked {
        block.takeIf { mutableState.value == BusState.Running }?.invoke()
    }

    suspend fun drain(timeout: Duration): DrainResult {
        val isBegun = lock.locked { mutableState.compareAndSet(expect = BusState.Running, update = BusState.Draining) }
        if (!isBegun) return termination.await()
        announce(BusState.Draining)
        val before = environment.lost
        val drained = withTimeoutOrNull(timeout) { directory.runtimes.forEach { runtime -> runtime.drain() } }
        termination.complete(outcome(drained, before))
        return termination.await()
    }

    suspend fun close(): CloseReport {
        val before = environment.lost
        val isClosed = closeNow()
        join()
        if (!isClosed) return CloseReport(dropped = 0)
        return CloseReport(dropped = environment.lost - before)
    }

    fun closeNow(): Boolean {
        if (!abort(DropReason.Closed)) return false
        termination.complete(DrainResult.AlreadyClosed)
        return true
    }

    private fun abort(reason: DropReason): Boolean {
        if (!switchToClosed()) return false
        scopeWatch.load()?.dispose()
        directory.runtimes.forEach { runtime -> runtime.abort(reason) }
        parent.cancel()
        announce(BusState.Closed)
        return true
    }

    private suspend fun outcome(drained: Unit?, before: Long): DrainResult {
        if (drained != null) {
            finish()
            return DrainResult.Completed
        }
        abort(DropReason.Abandoned)
        join()
        return DrainResult.TimedOut(environment.lost - before)
    }

    private fun finish() {
        if (!switchToClosed()) return
        scopeWatch.load()?.dispose()
        parent.cancel()
        announce(BusState.Closed)
    }

    private fun switchToClosed(): Boolean = lock.locked {
        val previous = mutableState.value
        mutableState.value = BusState.Closed
        previous != BusState.Closed
    }

    private suspend fun join() {
        parent.coroutineContext.job.join()
    }

    private fun announce(state: BusState) {
        environment.observers.announce { BusObservation.StateChanged(state) }
    }
}
