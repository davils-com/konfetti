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

private const val INIT_ZERO: Long = 0x736f6d6570736575
private const val INIT_ONE: Long = 0x646f72616e646f6d
private const val INIT_TWO: Long = 0x6c7967656e657261
private const val INIT_THREE: Long = 0x7465646279746573
private const val FINAL_MARK: Long = 0xff
private const val COMPRESSION_ROUNDS: Int = 2
private const val FINALIZATION_ROUNDS: Int = 4
private const val ROTATE_A: Int = 13
private const val ROTATE_B: Int = 32
private const val ROTATE_C: Int = 16
private const val ROTATE_D: Int = 21
private const val ROTATE_E: Int = 17

internal class SipState(key: SipKey) {
    private var v0: Long = key.first xor INIT_ZERO
    private var v1: Long = key.second xor INIT_ONE
    private var v2: Long = key.first xor INIT_TWO
    private var v3: Long = key.second xor INIT_THREE

    fun absorb(message: Long) {
        v3 = v3 xor message
        repeat(COMPRESSION_ROUNDS) { round() }
        v0 = v0 xor message
    }

    fun finish(): Long {
        v2 = v2 xor FINAL_MARK
        repeat(FINALIZATION_ROUNDS) { round() }
        return v0 xor v1 xor v2 xor v3
    }

    private fun round() {
        v0 += v1
        v1 = v1.rotateLeft(ROTATE_A) xor v0
        v0 = v0.rotateLeft(ROTATE_B)
        v2 += v3
        v3 = v3.rotateLeft(ROTATE_C) xor v2
        v0 += v3
        v3 = v3.rotateLeft(ROTATE_D) xor v0
        v2 += v1
        v1 = v1.rotateLeft(ROTATE_E) xor v2
        v2 = v2.rotateLeft(ROTATE_B)
    }
}
