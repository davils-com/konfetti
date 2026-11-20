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

package com.davils.konfetti.core.topic

import com.davils.kore.core.naming.NameRule
import com.davils.kore.core.naming.nameRule
import kotlin.jvm.JvmInline

private const val KIND: String = "topic name"

private val topicNames: NameRule = nameRule {
    segmentSeparator = '.'
    example = "inventory.devices"
}

/**
 * The name of a topic, such as `inventory.devices`: lower-case words of letters, digits and dashes that start with a
 * letter, joined by dots, `[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*)*`, at most [MAX_LENGTH] characters long.
 *
 * The name `konfetti.dead-letters` is [reserved][DEAD_LETTERS] for the dead letters of a bus.
 *
 * @property value the name.
 * @constructor Checks and wraps a name.
 * @throws IllegalArgumentException if [value] is no topic name; the message shows the text escaped and shortened.
 * @since 1.0.0
 */
@JvmInline
public value class TopicName(public val value: String) : Comparable<TopicName> {
    init {
        topicNames.requireValid(text = value, kind = KIND)
    }

    /**
     * Compares the names by their text, character by character.
     *
     * @param other the name to compare with.
     * @return a negative number, zero or a positive number as this name sorts before, with or after [other].
     * @since 1.0.0
     */
    override fun compareTo(other: TopicName): Int = value.compareTo(other.value)

    /**
     * Returns the name itself.
     *
     * @return [value].
     * @since 1.0.0
     */
    override fun toString(): String = value

    /**
     * The limits and the reserved names of topic names.
     *
     * @since 1.0.0
     */
    public companion object {
        /**
         * The most characters of a topic name: 256.
         *
         * @since 1.0.0
         */
        public const val MAX_LENGTH: Int = 256

        /**
         * The name of the topic every bus declares for its dead letters, `konfetti.dead-letters`.
         *
         * No other topic may take it.
         *
         * @since 1.0.0
         */
        public val DEAD_LETTERS: TopicName = TopicName("konfetti.dead-letters")

        /**
         * Tells whether [text] is a topic name.
         *
         * @param text the text to check; it may come from an attacker.
         * @return true if [text] follows the grammar of a topic name and is at most [MAX_LENGTH] characters long.
         * @since 1.0.0
         */
        public fun isValid(text: String): Boolean = topicNames.matches(text)
    }
}
