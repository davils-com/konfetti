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

import com.davils.konfetti.core.bus.Entropy
import com.davils.kore.core.identity.UuidV7
import com.davils.kore.core.identity.generator.UuidGenerator
import kotlin.time.TimeSource

internal data class BusSources(
    val identifiers: UuidGenerator<UuidV7>,
    val timeSource: TimeSource.WithComparableMarks,
    val entropy: Entropy,
)
