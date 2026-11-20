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

package com.davils.konfetti.core.internal

import com.davils.kore.core.annotation.DavilsInternal
import com.davils.kore.core.text.SafeText
import kotlin.reflect.KClass

private const val UNNAMED: String = "<anonymous>"
private const val MAX_TYPE_NAME_LENGTH: Int = 128

@OptIn(DavilsInternal::class)
internal val KClass<*>.displayName: String get() = SafeText.text(simpleName ?: UNNAMED, MAX_TYPE_NAME_LENGTH)

@OptIn(DavilsInternal::class)
internal fun shown(text: CharSequence, maxLength: Int): String = SafeText.text(text, maxLength)

@OptIn(DavilsInternal::class)
internal val redacted: String get() = SafeText.REDACTED
