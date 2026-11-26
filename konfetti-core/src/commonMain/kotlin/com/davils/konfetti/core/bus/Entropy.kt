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

package com.davils.konfetti.core.bus

import com.davils.kore.core.random.Entropy as KoreEntropy

/**
 * The source of the randomness of a bus: the jitter of exponential backoffs and the secret key of the hash that maps
 * ordering keys to partitions.
 *
 * The default draws from `SecureEntropy` of Kore, the secure random generator of the platform. A test may
 * set a predictable source to make jitter and partitions repeatable; production code keeps the default, because a
 * predictable key lets an attacker choose ordering keys that crowd one partition.
 *
 * A bus calls the source under a lock, so the source needs no synchronization of its own. A source that throws is
 * contained: the bus then uses no jitter, or the secure default for the key.
 *
 * Since 1.1.0 every source of a bus is an [Entropy of Kore][KoreEntropy] as well, so it offers the bounded numbers,
 * bytes and fractions of Kore and can be handed to every Davils library that takes one.
 *
 * @since 1.0.0
 */
public fun interface Entropy : KoreEntropy {
    /**
     * Returns 64 random bits.
     *
     * @return the bits.
     * @since 1.0.0
     */
    override fun nextLong(): Long
}
