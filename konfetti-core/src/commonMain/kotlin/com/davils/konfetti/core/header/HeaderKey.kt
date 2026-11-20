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

import com.davils.konfetti.core.internal.header.BooleanCodec
import com.davils.konfetti.core.internal.header.LongCodec
import com.davils.konfetti.core.internal.header.TextCodec
import com.davils.konfetti.core.internal.header.UuidCodec
import com.davils.kore.core.identity.Uuid

/**
 * The typed key of a header: its name, the codec of its value and whether the value is sensitive.
 *
 * The value of a sensitive header is replaced by `REDACTED` wherever headers are shown, in [EventHeaders.toString] and
 * in the text of an envelope; it still travels to every consumer unchanged.
 *
 * @param T the type of the value.
 * @property name the name of the header.
 * @property codec the codec of the value.
 * @property isSensitive whether the value must never be shown; false by default.
 * @constructor Creates a key with its own codec.
 * @since 1.0.0
 */
public class HeaderKey<T : Any>(
    public val name: HeaderName,
    public val codec: HeaderCodec<T>,
    public val isSensitive: Boolean = false,
) {
    /**
     * Makes an entry of this header with [value].
     *
     * @param value the value.
     * @return the entry.
     * @since 1.0.0
     */
    public operator fun invoke(value: T): HeaderEntry<T> = HeaderEntry(this, value)

    /**
     * Shows the key as `HeaderKey(name)`.
     *
     * @return the key as text.
     * @since 1.0.0
     */
    override fun toString(): String = "HeaderKey($name)"

    /**
     * Keys with the built-in codecs.
     *
     * @since 1.0.0
     */
    public companion object {
        /**
         * A key whose value is text, kept as it is.
         *
         * @param name the name of the header.
         * @param sensitive whether the value must never be shown.
         * @return the key.
         * @throws IllegalArgumentException if [name] is no [HeaderName].
         * @since 1.0.0
         */
        public fun text(name: String, sensitive: Boolean = false): HeaderKey<String> =
            HeaderKey(HeaderName(name), TextCodec, sensitive)

        /**
         * A key whose value is a [Long], written in decimal.
         *
         * @param name the name of the header.
         * @param sensitive whether the value must never be shown.
         * @return the key.
         * @throws IllegalArgumentException if [name] is no [HeaderName].
         * @since 1.0.0
         */
        public fun long(name: String, sensitive: Boolean = false): HeaderKey<Long> =
            HeaderKey(HeaderName(name), LongCodec, sensitive)

        /**
         * A key whose value is a [Boolean], written as `true` or `false`.
         *
         * @param name the name of the header.
         * @param sensitive whether the value must never be shown.
         * @return the key.
         * @throws IllegalArgumentException if [name] is no [HeaderName].
         * @since 1.0.0
         */
        public fun boolean(name: String, sensitive: Boolean = false): HeaderKey<Boolean> =
            HeaderKey(HeaderName(name), BooleanCodec, sensitive)

        /**
         * A key whose value is a [Uuid], written in its canonical form.
         *
         * @param name the name of the header.
         * @param sensitive whether the value must never be shown.
         * @return the key.
         * @throws IllegalArgumentException if [name] is no [HeaderName].
         * @since 1.0.0
         */
        public fun uuid(name: String, sensitive: Boolean = false): HeaderKey<Uuid> =
            HeaderKey(HeaderName(name), UuidCodec, sensitive)
    }
}
