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

/**
 * Turns the value of a header into text and back.
 *
 * Headers travel as text, so a header key carries the codec of its value. [decode] reads text that any publisher may
 * have written, so it must return null for text it cannot read instead of throwing; a codec that throws anyway is
 * contained, and [EventHeaders.get] then returns null.
 *
 * @param T the type of the value.
 * @since 1.0.0
 */
public interface HeaderCodec<T : Any> {
    /**
     * Writes [value] as text.
     *
     * @param value the value to write.
     * @return the text of [value].
     * @since 1.0.0
     */
    public fun encode(value: T): String

    /**
     * Reads a value from [text].
     *
     * @param text the text to read; it may come from any publisher.
     * @return the value, or null if [text] is no value of this codec.
     * @since 1.0.0
     */
    public fun decode(text: String): T?
}
