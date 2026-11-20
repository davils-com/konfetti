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

package com.davils.konfetti.core.internal.partition

private const val CHARS_PER_WORD: Int = 4
private const val BITS_PER_CHAR: Int = 16
private const val BYTES_PER_CHAR: Int = 2
private const val LENGTH_SHIFT: Int = 56
private const val LENGTH_MASK: Long = 0xff

internal fun sipHash(key: SipKey, text: String): Long {
    val state = SipState(key)
    val words = text.length / CHARS_PER_WORD
    for (word in 0 until words) {
        state.absorb(wordAt(text = text, start = word * CHARS_PER_WORD, count = CHARS_PER_WORD))
    }
    val tailStart = words * CHARS_PER_WORD
    val length = text.length.toLong() * BYTES_PER_CHAR and LENGTH_MASK
    val tail = wordAt(text = text, start = tailStart, count = text.length - tailStart)
    state.absorb(length shl LENGTH_SHIFT or tail)
    return state.finish()
}

private fun wordAt(text: String, start: Int, count: Int): Long {
    var word = 0L
    for (offset in 0 until count) {
        word = word or (text[start + offset].code.toLong() shl BITS_PER_CHAR * offset)
    }
    return word
}
