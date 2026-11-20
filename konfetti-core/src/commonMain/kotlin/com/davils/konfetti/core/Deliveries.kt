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

import com.davils.konfetti.core.internal.delivery.HandledEnvelope
import kotlinx.coroutines.currentCoroutineContext

/**
 * Returns the envelope the calling handler is delivering, or null outside a handler.
 *
 * A publication from inside a handler takes this envelope as its cause unless it names another.
 *
 * @return the envelope being handled, or null.
 * @since 1.0.0
 */
public suspend fun currentEnvelope(): Envelope<Event>? = currentCoroutineContext()[HandledEnvelope]?.envelope

/**
 * Returns the number of the attempt the calling handler runs in, starting at 1, or null outside a handler.
 *
 * @return the attempt, or null.
 * @since 1.0.0
 */
public suspend fun currentAttempt(): Int? = currentCoroutineContext()[HandledEnvelope]?.attempt
