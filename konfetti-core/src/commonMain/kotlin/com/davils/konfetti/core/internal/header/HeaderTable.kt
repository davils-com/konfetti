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

package com.davils.konfetti.core.internal.header

import com.davils.konfetti.core.header.HeaderName
import com.davils.konfetti.core.internal.redacted
import com.davils.konfetti.core.internal.shown

private const val MAX_SHOWN_VALUE_LENGTH: Int = 256
private const val MAX_SHOWN_HEADERS: Int = 32

internal class HeaderTable private constructor(
    private val keys: Array<HeaderName>,
    private val values: Array<String>,
    private val sensitive: BooleanArray,
) {
    val size: Int get() = keys.size

    val names: List<HeaderName> get() = keys.asList()

    val longestValue: Int get() = values.maxOfOrNull { value -> value.length } ?: 0

    fun valueOf(name: HeaderName): String? {
        val index = indexOf(name)
        if (index < 0) return null
        return values[index]
    }

    fun isSensitive(name: HeaderName): Boolean {
        val index = indexOf(name)
        if (index < 0) return false
        return sensitive[index]
    }

    fun with(name: HeaderName, value: String, isSensitive: Boolean): HeaderTable {
        val index = indexOf(name)
        if (index >= 0) return replaced(index, value, isSensitive)
        return inserted(at = -index - 1, name = name, value = value, isSensitive = isSensitive)
    }

    fun without(name: HeaderName): HeaderTable {
        val index = indexOf(name)
        val remaining = keys.indices.filter { position -> position != index }
        return HeaderTable(
            keys = Array(remaining.size) { position -> keys[remaining[position]] },
            values = Array(remaining.size) { position -> values[remaining[position]] },
            sensitive = BooleanArray(remaining.size) { position -> sensitive[remaining[position]] },
        )
    }

    fun plus(other: HeaderTable): HeaderTable = other.keys.indices.fold(this) { table, index ->
        table.with(other.keys[index], other.values[index], other.sensitive[index])
    }

    override fun equals(other: Any?): Boolean {
        if (other !is HeaderTable) return false
        return keys.contentEquals(other.keys) &&
            values.contentEquals(other.values) &&
            sensitive.contentEquals(other.sensitive)
    }

    override fun hashCode(): Int {
        val named = keys.contentHashCode() * HASH_FACTOR + values.contentHashCode()
        return named * HASH_FACTOR + sensitive.contentHashCode()
    }

    override fun toString(): String {
        val shownCount = minOf(a = size, b = MAX_SHOWN_HEADERS)
        val parts = (0 until shownCount).map { index -> render(index) }
        val omitted = size - shownCount
        val tail = listOfNotNull("… $omitted more".takeIf { omitted > 0 })
        return (parts + tail).joinToString(prefix = "{", postfix = "}")
    }

    private fun render(index: Int): String {
        if (sensitive[index]) return "${keys[index]}=$redacted"
        return "${keys[index]}=${shown(values[index], MAX_SHOWN_VALUE_LENGTH)}"
    }

    private fun replaced(index: Int, value: String, isSensitive: Boolean): HeaderTable {
        val nextValues = values.copyOf()
        nextValues[index] = value
        val nextSensitive = sensitive.copyOf()
        nextSensitive[index] = isSensitive
        return HeaderTable(keys = keys, values = nextValues, sensitive = nextSensitive)
    }

    private fun inserted(at: Int, name: HeaderName, value: String, isSensitive: Boolean): HeaderTable {
        val count = size + 1
        return HeaderTable(
            keys = Array(count) { position -> pick(position, at, name) { index -> keys[index] } },
            values = Array(count) { position -> pick(position, at, value) { index -> values[index] } },
            sensitive = BooleanArray(
                count,
            ) { position -> pick(position, at, isSensitive) { index -> sensitive[index] } },
        )
    }

    private fun indexOf(name: HeaderName): Int {
        var low = 0
        var high = size - 1
        while (low <= high) {
            val middle = low + high ushr 1
            val order = keys[middle].compareTo(name)
            if (order == 0) return middle
            if (order < 0) low = middle + 1
            if (order > 0) high = middle - 1
        }
        return -(low + 1)
    }

    companion object {
        private const val HASH_FACTOR: Int = 31

        val EMPTY: HeaderTable = HeaderTable(emptyArray(), emptyArray(), BooleanArray(0))
    }
}

private inline fun <T> pick(position: Int, at: Int, inserted: T, existing: (Int) -> T): T {
    if (position == at) return inserted
    return existing(position - (position - at).coerceIn(minimumValue = 0, maximumValue = 1))
}
