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

package com.davils.konfetti.core.fixture

import com.davils.konfetti.core.consume.Consumption
import com.davils.kore.core.validation.violation.ViolationCode
import io.kotest.matchers.types.shouldBeInstanceOf

internal val Consumption.codes: List<ViolationCode>
    get() {
        val invalid = shouldBeInstanceOf<Consumption.Invalid>()
        return invalid.report.errors.map { violation -> violation.code }
    }
