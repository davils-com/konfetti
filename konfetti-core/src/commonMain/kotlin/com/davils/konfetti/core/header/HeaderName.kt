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

import com.davils.kore.core.naming.NameRule
import com.davils.kore.core.naming.NameStart
import com.davils.kore.core.naming.nameRule
import kotlin.jvm.JvmInline

private const val KIND: String = "header name"

private val headerNames: NameRule = nameRule {
    start = NameStart.LetterOrDigit
    separators = "-."
    isStrict = true
    example = "trace-id"
}

/**
 * The name of a header, such as `trace-id`: lower-case letters and digits, joined by single dashes or dots,
 * `[a-z0-9]+([.-][a-z0-9]+)*`, at most [MAX_LENGTH] characters long.
 *
 * @property value the name.
 * @constructor Checks and wraps a name.
 * @throws IllegalArgumentException if [value] is no header name; the message shows the text escaped and shortened.
 * @since 1.0.0
 */
@JvmInline
public value class HeaderName(public val value: String) : Comparable<HeaderName> {
    init {
        headerNames.requireValid(text = value, kind = KIND)
    }

    /**
     * Compares the names by their text, character by character.
     *
     * @param other the name to compare with.
     * @return a negative number, zero or a positive number as this name sorts before, with or after [other].
     * @since 1.0.0
     */
    override fun compareTo(other: HeaderName): Int = value.compareTo(other.value)

    /**
     * Returns the name itself.
     *
     * @return [value].
     * @since 1.0.0
     */
    override fun toString(): String = value

    /**
     * The limits of header names.
     *
     * @since 1.0.0
     */
    public companion object {
        /**
         * The most characters of a header name: 256.
         *
         * @since 1.0.0
         */
        public const val MAX_LENGTH: Int = 256

        /**
         * Tells whether [text] is a header name.
         *
         * @param text the text to check; it may come from an attacker.
         * @return true if [text] follows the grammar of a header name and is at most [MAX_LENGTH] characters long.
         * @since 1.0.0
         */
        public fun isValid(text: String): Boolean = headerNames.matches(text)
    }
}
