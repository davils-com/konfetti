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

import com.davils.konfetti.core.limit.BusLimits
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldEndWith

private val failing = HeaderKey(HeaderName("failing"), object : HeaderCodec<String> {
    override fun encode(value: String): String = value

    override fun decode(text: String): String = error("cannot decode")
})

private fun many(count: Int): EventHeaders =
    (0 until count).fold(EventHeaders.EMPTY) { headers, index -> headers.with(HeaderKey.long("h$index")(index.toLong())) }

class HeaderBoundsTest : FunSpec({
    context("headers") {
        test("hold at most the hard number of entries") {
            val full = many(BusLimits.HARD_HEADERS)

            full.size shouldBe BusLimits.HARD_HEADERS
            shouldThrow<IllegalArgumentException> { full.with(HeaderKey.text("one-more")("x")) }
            shouldThrow<IllegalArgumentException> { full + headersOf(HeaderKey.text("one-more")("x")) }
            full.with(HeaderKey.long("h0")(1))[HeaderKey.long("h0")] shouldBe 1
        }

        test("refuse a value longer than the hard length") {
            val key = HeaderKey.text("payload")

            headersOf(key("x".repeat(BusLimits.HARD_HEADER_VALUE_LENGTH))).size shouldBe 1
            shouldThrow<IllegalArgumentException> {
                headersOf(key("x".repeat(BusLimits.HARD_HEADER_VALUE_LENGTH + 1)))
            }.message shouldContain "payload"
        }

        test("find every entry of many, in sorted order") {
            val headers = many(100)

            (0 until 100).forEach { index -> headers[HeaderKey.long("h$index")] shouldBe index.toLong() }
            headers.names shouldBe headers.names.sorted()
            headers.textOf(HeaderName("h7")) shouldBe "7"
            headers.textOf(HeaderName("absent")) shouldBe null
        }

        test("contain a codec that fails") {
            val headers = headersOf(failing("anything"))

            headers[failing] shouldBe null
            headers.textOf(HeaderName("failing")) shouldBe "anything"
        }

        test("merge and replace sensitivity from the right") {
            val plain = HeaderKey.text("token")
            val secret = HeaderKey.text("token", sensitive = true)
            val left = headersOf(plain("a"), HeaderKey.text("left")("l"))
            val right = headersOf(secret("b"), HeaderKey.text("right")("r"))

            val merged = left + right

            merged.names.map(HeaderName::value) shouldContainExactly listOf("left", "right", "token")
            merged.isSensitive(HeaderName("token")) shouldBe true
            merged[plain] shouldBe "b"
            (EventHeaders.EMPTY + right) shouldBe right
            (left + EventHeaders.EMPTY) shouldBe left
            merged.isSensitive(HeaderName("absent")) shouldBe false
            merged.without(HeaderKey.text("absent")) shouldBe merged
            merged.without(plain).names.map(HeaderName::value) shouldContainExactly listOf("left", "right")
        }

        test("cut their text off after the first entries") {
            val text = many(40).toString()

            text shouldEndWith "… 8 more}"
            EventHeaders.EMPTY.toString() shouldBe "{}"
        }

        test("compare and hash by names, values and sensitivity") {
            val key = HeaderKey.text("token")

            headersOf(key("a")) shouldBe headersOf(key("a"))
            headersOf(key("a")).hashCode() shouldBe headersOf(key("a")).hashCode()
            (headersOf(key("a")) == headersOf(HeaderKey.text("token", sensitive = true)("a"))) shouldBe false
            (headersOf(key("a")) == headersOf(key("b"))) shouldBe false
            EventHeaders.EMPTY.equals("headers") shouldBe false
            (headersOf(key("a")) == headersOf(HeaderKey.text("other")("a"))) shouldBe false
            val varied = headersOf(key("a"), HeaderKey.text("longer")("abcdef"), HeaderKey.text("mid")("abc"))
            varied.exceeds(BusLimits.Default) shouldBe false
            varied.exceeds(com.davils.konfetti.core.limit.busLimits { maximumHeaderValueLength = 5 }) shouldBe true
        }
    }

    context("header names and keys") {
        test("follow their grammar") {
            HeaderName.isValid("trace-id") shouldBe true
            HeaderName.isValid("9to5.x") shouldBe true
            HeaderName.isValid("Trace") shouldBe false
            HeaderName.isValid("a--b") shouldBe false
            HeaderName.isValid("a".repeat(HeaderName.MAX_LENGTH + 1)) shouldBe false
            HeaderKey.boolean("flag").toString() shouldBe "HeaderKey(flag)"
            (HeaderName("b") > HeaderName("a")) shouldBe true
        }
    }
})
