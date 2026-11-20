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

import com.davils.konfetti.core.internal.header.HeaderTable
import com.davils.konfetti.core.limit.BusLimits
import com.davils.kore.core.failure.contained

/**
 * The headers of an envelope: an immutable set of named, encoded values.
 *
 * Headers hold at most [BusLimits.HARD_HEADERS] entries whose encoded values are at most
 * [BusLimits.HARD_HEADER_VALUE_LENGTH] characters long; a bus may allow less with [BusLimits.maximumHeaders] and
 * [BusLimits.maximumHeaderValueLength]. The entries are kept sorted by name, so every lookup takes logarithmic time
 * whatever the names, and adding or removing an entry copies the headers.
 *
 * Headers are an immutable value and safe to share between threads; two headers are equal when they hold the same
 * names, values and sensitive names.
 *
 * @since 1.0.0
 */
public class EventHeaders private constructor(private val table: HeaderTable) {
    /**
     * The names of the headers, sorted.
     *
     * @since 1.0.0
     */
    public val names: List<HeaderName> get() = table.names

    /**
     * The number of headers.
     *
     * @since 1.0.0
     */
    public val size: Int get() = table.size

    /**
     * Whether there are no headers.
     *
     * @since 1.0.0
     */
    public val isEmpty: Boolean get() = table.size == 0

    /**
     * Reads the value of the header of [key].
     *
     * @param T the type of the value.
     * @param key the key of the header.
     * @return the value, or null if the header is missing, its text is no value of the codec of [key], or the codec
     * failed.
     * @since 1.0.0
     */
    public operator fun <T : Any> get(key: HeaderKey<T>): T? {
        val text = table.valueOf(key.name) ?: return null
        return contained { key.codec.decode(text) }.getOrNull()
    }

    /**
     * Reads the encoded text of the header named [name].
     *
     * @param name the name of the header.
     * @return the text, or null if the header is missing.
     * @since 1.0.0
     */
    public fun textOf(name: HeaderName): String? = table.valueOf(name)

    /**
     * Tells whether a header of [key] is present.
     *
     * @param key the key of the header.
     * @return true if the headers hold a header with the name of [key].
     * @since 1.0.0
     */
    public operator fun contains(key: HeaderKey<*>): Boolean = table.valueOf(key.name) != null

    /**
     * Tells whether the header named [name] is sensitive.
     *
     * @param name the name of the header.
     * @return true if the header is present and its value must never be shown.
     * @since 1.0.0
     */
    public fun isSensitive(name: HeaderName): Boolean = table.isSensitive(name)

    /**
     * Returns these headers with [entry] added, replacing a header of the same name.
     *
     * @param entry the entry to add.
     * @return the new headers.
     * @throws IllegalArgumentException if the headers would hold more than [BusLimits.HARD_HEADERS] entries, or the
     * encoded value is longer than [BusLimits.HARD_HEADER_VALUE_LENGTH].
     * @since 1.0.0
     */
    public fun with(entry: HeaderEntry<*>): EventHeaders {
        val encoded = entry.encoded()
        require(encoded.length <= BusLimits.HARD_HEADER_VALUE_LENGTH) {
            "The value of header '${entry.key.name}' must be at most ${BusLimits.HARD_HEADER_VALUE_LENGTH} " +
                "characters long, but was ${encoded.length}"
        }
        val next = table.with(entry.key.name, encoded, entry.key.isSensitive)
        require(next.size <= BusLimits.HARD_HEADERS) {
            "Headers hold at most ${BusLimits.HARD_HEADERS} entries"
        }
        return EventHeaders(next)
    }

    /**
     * Returns these headers without the header of [key].
     *
     * @param key the key of the header to remove.
     * @return the new headers, or these headers if the header is missing.
     * @since 1.0.0
     */
    public fun without(key: HeaderKey<*>): EventHeaders {
        if (key !in this) return this
        return EventHeaders(table.without(key.name))
    }

    /**
     * Returns these headers with every header of [other] added; a header of [other] replaces a header of the same name,
     * including whether it is sensitive.
     *
     * @param other the headers to add.
     * @return the new headers.
     * @throws IllegalArgumentException if the headers would hold more than [BusLimits.HARD_HEADERS] entries.
     * @since 1.0.0
     */
    public operator fun plus(other: EventHeaders): EventHeaders {
        if (other.isEmpty) return this
        val merged = table.plus(other.table)
        require(merged.size <= BusLimits.HARD_HEADERS) {
            "Headers hold at most ${BusLimits.HARD_HEADERS} entries"
        }
        return EventHeaders(merged)
    }

    internal fun exceeds(limits: BusLimits): Boolean =
        table.size > limits.maximumHeaders || table.longestValue > limits.maximumHeaderValueLength

    /**
     * Tells whether [other] holds the same names, values and sensitive names.
     *
     * @param other the object to compare with.
     * @return true if [other] are equal headers.
     * @since 1.0.0
     */
    override fun equals(other: Any?): Boolean = other is EventHeaders && table == other.table

    /**
     * Returns a hash of the names, values and sensitive names, consistent with [equals].
     *
     * @return the hash.
     * @since 1.0.0
     */
    override fun hashCode(): Int = table.hashCode()

    /**
     * Shows the headers sorted by name as `{name=value, …}`; values are escaped and shortened, the values of sensitive
     * headers are replaced by `REDACTED`, and long headers are cut off with the number of headers left out.
     *
     * @return the headers as text.
     * @since 1.0.0
     */
    override fun toString(): String = table.toString()

    /**
     * The empty headers.
     *
     * @since 1.0.0
     */
    public companion object {
        /**
         * Headers without any header.
         *
         * @since 1.0.0
         */
        public val EMPTY: EventHeaders = EventHeaders(HeaderTable.EMPTY)
    }
}
