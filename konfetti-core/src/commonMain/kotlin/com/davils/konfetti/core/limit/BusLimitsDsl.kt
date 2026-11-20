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

package com.davils.konfetti.core.limit

/**
 * Builds limits that start from [base] and change what [configure] sets, such as
 * `busLimits { maximumTopics = 64; maximumHeaders = 16 }`.
 *
 * @param base the limits every property starts from; [BusLimits.Default] by default.
 * @param configure the block that sets the limits to change.
 * @return the validated limits.
 * @throws com.davils.kore.core.validation.report.ValidationException if a count is below 1, a duration is not
 * positive, or a limit is above its hard ceiling; the report names every such limit.
 * @since 1.0.0
 */
public fun busLimits(base: BusLimits = BusLimits.Default, configure: BusLimitsBuilder.() -> Unit): BusLimits {
    val builder = BusLimitsBuilder()
    builder.from(base)
    builder.configure()
    return builder.build()
}
