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

import com.davils.konfetti.core.consume.ConsumerName
import com.davils.konfetti.core.consume.ConsumerState
import com.davils.konfetti.core.internal.bus.Observers
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.core.topic.TopicName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DisposableHandle
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
internal class ConsumerLifecycle(
    private val topic: TopicName,
    private val name: ConsumerName,
    private val observers: Observers,
    private val scope: CoroutineScope,
) {
    private val closing = AtomicReference<DropReason?>(null)
    private val owner = AtomicReference<DisposableHandle?>(null)
    private val mutableState = MutableStateFlow<ConsumerState>(ConsumerState.Running)

    val state: StateFlow<ConsumerState> = mutableState.asStateFlow()

    fun reason(): DropReason = closing.load() ?: DropReason.Closed

    fun watch(ownerScope: CoroutineScope?, detach: () -> Unit) {
        val job = ownerScope?.run { coroutineContext[Job] } ?: return
        owner.store(job.invokeOnCompletion { detach() })
    }

    fun beginDrain() {
        mutableState.compareAndSet(expect = ConsumerState.Running, update = ConsumerState.Draining)
    }

    fun isDraining(): Boolean = mutableState.value == ConsumerState.Draining

    fun detach() {
        terminate(ConsumerState.Detached, DropReason.Detached, BusObservation.ConsumerDetached(topic, name))
    }

    fun stop(cause: Throwable) {
        terminate(ConsumerState.Stopped(cause), DropReason.Stopped, BusObservation.ConsumerStopped(topic, name, cause))
    }

    fun abort(reason: DropReason) {
        terminate(ConsumerState.Detached, reason, announcement = null)
    }

    private fun terminate(next: ConsumerState, reason: DropReason, announcement: BusObservation?) {
        if (!closing.compareAndSet(null, reason)) return
        mutableState.value = next
        owner.load()?.dispose()
        scope.cancel()
        if (announcement != null) observers.announce { announcement }
    }
}
