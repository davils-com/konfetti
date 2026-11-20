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

package com.davils.konfetti.core.header

import com.davils.konfetti.core.internal.redacted
import com.davils.konfetti.core.internal.shown

private const val MAX_SHOWN_LENGTH: Int = 256

/**
 * A value for one header, made by [HeaderKey.invoke] and added with [EventHeaders.with] or [headersOf].
 *
 * @param T the type of the value.
 * @property key the key of the header.
 * @property value the value.
 * @since 1.0.0
 */
public class HeaderEntry<T : Any> internal constructor(public val key: HeaderKey<T>, public val value: T) {
    internal fun encoded(): String = key.codec.encode(value)

    /**
     * Shows the entry as `name=value`, with the value escaped and shortened, or replaced by `REDACTED` when the key is
     * sensitive.
     *
     * @return the entry as text.
     * @since 1.0.0
     */
    override fun toString(): String {
        if (key.isSensitive) return "${key.name}=$redacted"
        return "${key.name}=${shown(value.toString(), MAX_SHOWN_LENGTH)}"
    }
}
