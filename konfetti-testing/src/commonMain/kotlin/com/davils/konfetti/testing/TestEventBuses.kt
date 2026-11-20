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

import com.davils.konfetti.core.EventBus
import com.davils.konfetti.core.bus.EventBusBuilder
import com.davils.konfetti.core.eventBus
import com.davils.kore.testing.SequentialUuidV7Generator
import kotlinx.coroutines.CoroutineScope

private const val SEED: Long = 42

/**
 * Builds a bus for tests: its envelope ids come from a [SequentialUuidV7Generator] and its randomness from a
 * [SeededEntropy], so ids, jitter and partitions repeat from run to run; [block] may override both.
 *
 * Pair it with a test scope and set [EventBusBuilder.timeSource] to the time source of its scheduler to run retries
 * and timeouts in virtual time.
 *
 * @param scope the scope the bus runs in.
 * @param block declares the bus as [eventBus] does.
 * @return the running bus.
 * @throws com.davils.kore.core.validation.report.ValidationException if the settings are invalid.
 * @since 1.0.0
 */
public fun testEventBus(scope: CoroutineScope, block: EventBusBuilder.() -> Unit = {}): EventBus = eventBus(scope) {
    identifiers = SequentialUuidV7Generator()
    entropy = SeededEntropy(SEED)
    block()
}
