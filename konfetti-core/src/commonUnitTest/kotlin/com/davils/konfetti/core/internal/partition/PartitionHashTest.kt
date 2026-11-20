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

import com.davils.konfetti.core.topic.OrderingKey
import io.kotest.core.spec.style.FunSpec
import io.kotest.datatest.withData
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.ints.shouldBeInRange
import io.kotest.matchers.ints.shouldBeLessThan
import io.kotest.matchers.shouldBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.string
import io.kotest.property.checkAll

private const val PARTITIONS: Int = 8
private const val KEYS: Int = 8_000
private const val SHARE: Int = 800
private const val SAME_PARTITION_LIMIT: Int = 2_000
private const val REFERENCE_FIRST: Long = 0x0706050403020100
private const val REFERENCE_SECOND: Long = 0x0f0e0d0c0b0a0908
private const val OTHER_FIRST: Long = 1
private const val OTHER_SECOND: Long = 2

private val referenceKey: SipKey = SipKey(first = REFERENCE_FIRST, second = REFERENCE_SECOND)

private val referenceVectors: List<Pair<String, Long>> = listOf(
    "" to 0x726FDB47DD0E0E31,
    "\u0100" to 0xD6C8009D9A94F5A,
    "\u0100\u0302" to -0x30D86B1FD88E7849,
    "\u0100\u0302\u0504" to -0x3436B991A7011C32,
    "\u0100\u0302\u0504\u0706" to -0x6C0A0A86656CDB9E,
    "\u0100\u0302\u0504\u0706\u0908" to 0x7A5DBBC594DDB9F3,
    "\u0100\u0302\u0504\u0706\u0908\u0b0a" to 0x751E8FBC860EE5FB,
    "\u0100\u0302\u0504\u0706\u0908\u0b0a\u0d0c" to -0x8DC356F71850D12,
    "\u0100\u0302\u0504\u0706\u0908\u0b0a\u0d0c\u0f0e" to 0x3F2ACC7F57C29BDB,
)

class PartitionHashTest : FunSpec({
    context("SipHash-2-4 matches the reference vectors of its authors for messages of whole characters") {
        withData(nameFn = { vector -> "${vector.first.length * 2} bytes" }, ts = referenceVectors) { (text, hash) ->
            sipHash(key = referenceKey, text = text) shouldBe hash
        }
    }

    context("partitioning") {
        test("puts the same key on the same partition every time") {
            checkAll(
                genA = Arb.string(minSize = 1),
                genB = Arb.int(1..PARTITIONS),
                genC = Arb.long(),
                genD = Arb.long(),
            ) { key, partitions, first, second ->
                val ordering = OrderingKey(key)
                val secret = SipKey(first = first, second = second)

                partitionOf(ordering, partitions, secret) shouldBe partitionOf(ordering, partitions, secret)
                partitionOf(ordering, partitions, secret) shouldBeInRange (0 until partitions)
            }
        }

        test("spreads many keys over every partition") {
            val counts = IntArray(PARTITIONS)
            repeat(KEYS) { index ->
                counts[partitionOf(OrderingKey("device-$index"), PARTITIONS, referenceKey)] += 1
            }

            counts.forEach { count -> count shouldBeGreaterThan SHARE }
        }

        test("depends on the secret, so keys chosen for one bus do not crowd a partition of another") {
            val other = SipKey(first = OTHER_FIRST, second = OTHER_SECOND)
            val crowd = (0 until KEYS)
                .map { index -> OrderingKey("device-$index") }
                .filter { key -> partitionOf(key, PARTITIONS, referenceKey) == 0 }

            val stillTogether = crowd.count { key -> partitionOf(key, PARTITIONS, other) == 0 }

            stillTogether shouldBeLessThan SAME_PARTITION_LIMIT.coerceAtMost(crowd.size / 2)
        }

        test("puts everything on the only partition there is") {
            partitionOf(OrderingKey("anything"), 1, referenceKey) shouldBe 0
        }
    }
})
