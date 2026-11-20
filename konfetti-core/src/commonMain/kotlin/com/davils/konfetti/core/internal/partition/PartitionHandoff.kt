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
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi

private const val PENDING: Int = 0
private const val SENT: Int = 1
private const val LOST: Int = 2
private const val SETTLED: Int = 3

@OptIn(ExperimentalAtomicApi::class)
internal class PartitionHandoff<T : Event>(val envelope: Envelope<T>) {
    private val state = AtomicInt(PENDING)

    fun accept(): Boolean = state.compareAndSet(expectedValue = PENDING, newValue = SENT)

    fun undeliver(): Boolean {
        if (state.compareAndSet(expectedValue = PENDING, newValue = LOST)) return false
        return state.compareAndSet(expectedValue = SENT, newValue = SETTLED)
    }
}
