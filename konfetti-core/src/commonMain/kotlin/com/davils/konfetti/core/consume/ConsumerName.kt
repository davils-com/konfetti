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

package com.davils.konfetti.core.consume

import com.davils.kore.core.naming.NameRule
import com.davils.kore.core.naming.nameRule
import kotlin.jvm.JvmInline

private const val KIND: String = "consumer name"

internal val consumerNames: NameRule = nameRule {
    segmentSeparator = '.'
    example = "inventory.indexer"
}

/**
 * The name of a consumer, such as `inventory.indexer`: lower-case words of letters, digits and dashes that start with
 * a letter, joined by dots, `[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*)*`, at most [MAX_LENGTH] characters long.
 *
 * A name is unique among the consumers of one topic.
 *
 * @property value the name.
 * @constructor Checks and wraps a name.
 * @throws IllegalArgumentException if [value] is no consumer name; the message shows the text escaped and shortened.
 * @since 1.0.0
 */
@JvmInline
public value class ConsumerName(public val value: String) : Comparable<ConsumerName> {
    init {
        consumerNames.requireValid(text = value, kind = KIND)
    }

    /**
     * Compares the names by their text, character by character.
     *
     * @param other the name to compare with.
     * @return a negative number, zero or a positive number as this name sorts before, with or after [other].
     * @since 1.0.0
     */
    override fun compareTo(other: ConsumerName): Int = value.compareTo(other.value)

    /**
     * Returns the name itself.
     *
     * @return [value].
     * @since 1.0.0
     */
    override fun toString(): String = value

    /**
     * The limits of consumer names.
     *
     * @since 1.0.0
     */
    public companion object {
        /**
         * The most characters of a consumer name: 256.
         *
         * @since 1.0.0
         */
        public const val MAX_LENGTH: Int = 256

        /**
         * Tells whether [text] is a consumer name.
         *
         * @param text the text to check; it may come from an attacker.
         * @return true if [text] follows the grammar of a consumer name and is at most [MAX_LENGTH] characters long.
         * @since 1.0.0
         */
        public fun isValid(text: String): Boolean = consumerNames.matches(text)
    }
}
