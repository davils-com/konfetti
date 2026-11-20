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

package com.davils.konfetti.testing

import com.davils.konfetti.core.bus.Entropy
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi

private const val GOLDEN_GAMMA: Long = -0x61C8864680B583EB
private const val FIRST_MIX: Long = -0x40A7B892E31B1A47
private const val SECOND_MIX: Long = -0x6B2FB644ECCEEE15
private const val FIRST_SHIFT: Int = 30
private const val SECOND_SHIFT: Int = 27
private const val THIRD_SHIFT: Int = 31

/**
 * A predictable [Entropy] for tests: the SplitMix64 sequence of [seed], so jitter and partitions repeat from run to
 * run.
 *
 * Never use it in production: a predictable key lets an attacker choose ordering keys that crowd one partition. The
 * source is safe to share between threads.
 *
 * @property seed the start of the sequence.
 * @constructor Starts the sequence at [seed].
 * @since 1.0.0
 */
@OptIn(ExperimentalAtomicApi::class)
public class SeededEntropy(public val seed: Long) : Entropy {
    private val state = AtomicLong(seed)

    /**
     * Returns the next 64 bits of the sequence.
     *
     * @return the bits.
     * @since 1.0.0
     */
    override fun nextLong(): Long {
        var mixed = state.addAndFetch(GOLDEN_GAMMA)
        mixed = (mixed xor (mixed ushr FIRST_SHIFT)) * FIRST_MIX
        mixed = (mixed xor (mixed ushr SECOND_SHIFT)) * SECOND_MIX
        return mixed xor (mixed ushr THIRD_SHIFT)
    }

    /**
     * Shows the source with its seed.
     *
     * @return the source as text.
     * @since 1.0.0
     */
    override fun toString(): String = "SeededEntropy(seed=$seed)"
}
