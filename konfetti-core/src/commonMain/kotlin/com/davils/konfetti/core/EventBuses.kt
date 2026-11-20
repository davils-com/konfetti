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

package com.davils.konfetti.core

import com.davils.konfetti.core.bus.EventBusBuilder
import kotlinx.coroutines.CoroutineScope

/**
 * Builds and starts an event bus on [scope].
 *
 * The bus runs its coroutines in [scope] on its dispatcher and closes when [scope] ends. Every topic declared in
 * [block] exists when the bus is returned.
 *
 * @param scope the scope the bus runs in.
 * @param block declares the topics and sets the observers, interceptors, defaults, limits and sources of the bus.
 * @return the running bus.
 * @throws com.davils.kore.core.validation.report.ValidationException if the settings are invalid; the report names
 * every problem.
 * @since 1.0.0
 */
public fun eventBus(scope: CoroutineScope, block: EventBusBuilder.() -> Unit = {}): EventBus {
    val builder = EventBusBuilder()
    builder.block()
    return EventBus(builder.build(), scope)
}
