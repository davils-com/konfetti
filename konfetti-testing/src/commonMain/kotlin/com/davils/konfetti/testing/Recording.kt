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

internal class Recording<E : Any>(private val last: Link<E>?, val size: Int, val overflow: Long) {
    val items: List<E> get() = last?.toList(size).orEmpty()

    fun with(item: E, capacity: Int): Recording<E> {
        if (size >= capacity) return Recording(last = last, size = size, overflow = overflow + 1)
        return Recording(last = Link(item, last), size = size + 1, overflow = overflow)
    }

    companion object {
        fun <E : Any> empty(): Recording<E> = Recording(last = null, size = 0, overflow = 0)
    }
}
