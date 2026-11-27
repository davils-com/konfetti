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
import com.davils.kore.core.identity.Uuid
import com.davils.kore.testing.SequentialUuidV7Generator
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.property.Arb
import io.kotest.property.arbitrary.boolean
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.string
import io.kotest.property.checkAll

private const val ATTEMPTS: Long = 3
private const val TRACE_ID: String = "trace-id"
private const val API_TOKEN: String = "api-token"
private const val SECRET: String = "secret"
private const val PASSWORD: String = "hunter2"
private val traceId = HeaderKey.text(TRACE_ID)
private val token = HeaderKey.text(API_TOKEN, sensitive = true)
private val attempts = HeaderKey.long("attempts")
private val replayed = HeaderKey.boolean("replayed")
private val request = HeaderKey.uuid("request-id")

class EventHeadersTest : FunSpec({
    context("reading") {
        test("returns what was set under a typed key") {
            val id: Uuid = SequentialUuidV7Generator().next()
            val headers = headersOf(traceId("t-1"), attempts(ATTEMPTS), replayed(true), request(id))

            headers[traceId] shouldBe "t-1"
            headers[attempts] shouldBe ATTEMPTS
            headers[replayed] shouldBe true
            headers[request] shouldBe id
        }

        test("returns nothing for a header that is not set") {
            EventHeaders.EMPTY[traceId] shouldBe null
            (traceId in EventHeaders.EMPTY) shouldBe false
            EventHeaders.EMPTY.isEmpty shouldBe true
        }

        test("returns nothing when the value does not decode under the key that reads it") {
            val text = HeaderKey.text("attempts")
            val headers = headersOf(text("many"))

            headers[attempts] shouldBe null
            (attempts in headers) shouldBe true
        }

        test("lists the names that are set, sorted") {
            headersOf(traceId("t-1"), token(SECRET)).names shouldContainExactly
                listOf(HeaderName(API_TOKEN), HeaderName(TRACE_ID))
        }
    }

    context("changing") {
        test("replaces a header that is set again") {
            val headers = headersOf(traceId("t-1")).with(traceId("t-2"))

            headers[traceId] shouldBe "t-2"
        }

        test("removes a header and ignores one that is not set") {
            val headers = headersOf(traceId("t-1"), token(SECRET))

            headers.without(traceId)[traceId] shouldBe null
            EventHeaders.EMPTY.without(traceId) shouldBe EventHeaders.EMPTY
        }

        test("lays other headers over these, the other side winning") {
            val combined = headersOf(traceId("t-1"), attempts(1)) + headersOf(traceId("t-2"))

            combined[traceId] shouldBe "t-2"
            combined[attempts] shouldBe 1
            combined + EventHeaders.EMPTY shouldBe combined
            EventHeaders.EMPTY + combined shouldBe combined
        }

        test("takes the sensitivity of the side that wins") {
            val plain = HeaderKey.text(API_TOKEN)
            val combined = headersOf(token(SECRET)) + headersOf(plain("visible"))

            combined.isSensitive(token.name) shouldBe false
        }
    }

    context("secrecy") {
        test("masks a sensitive header when rendered but returns it when asked for") {
            val headers = headersOf(traceId("t-1"), token(PASSWORD))

            headers.toString() shouldBe "{api-token=****, trace-id=t-1}"
            headers[token] shouldBe PASSWORD
            headers.isSensitive(token.name) shouldBe true
        }

        test("masks a sensitive entry and shows a plain one") {
            token(PASSWORD).toString() shouldBe "api-token=****"
            traceId("t-1").toString() shouldBe "trace-id=t-1"
        }

        test("drops the mask when a header is set again without being sensitive") {
            val plain = HeaderKey.text(API_TOKEN)

            val replaced = headersOf(token(PASSWORD)).with(plain("visible"))

            replaced.isSensitive(token.name) shouldBe false
        }
    }

    context("equality") {
        test("compares values and sensitivity") {
            headersOf(traceId("t-1")) shouldBe headersOf(traceId("t-1"))
            headersOf(traceId("t-1")).hashCode() shouldBe headersOf(traceId("t-1")).hashCode()
            headersOf(traceId("t-1")) shouldNotBe headersOf(traceId("t-2"))
            headersOf(HeaderKey.text(API_TOKEN)("x")) shouldNotBe headersOf(token("x"))
            headersOf(traceId("t-1")).equals("t-1") shouldBe false
        }

        test("never takes another type for the table of headers") {
            HeaderTable.EMPTY.equals(EventHeaders.EMPTY) shouldBe false
            HeaderTable.EMPTY shouldBe HeaderTable.EMPTY
        }
    }

    context("codecs") {
        test("bring every text, number and flag back unchanged") {
            checkAll(Arb.string(), Arb.long(), Arb.boolean()) { text, number, flag ->
                val headers = headersOf(traceId(text), attempts(number), replayed(flag))

                headers[traceId] shouldBe text
                headers[attempts] shouldBe number
                headers[replayed] shouldBe flag
            }
        }

        test("refuse a value that is not theirs") {
            val raw = HeaderKey.text("request-id")
            val headers = headersOf(raw("not-a-uuid"), HeaderKey.text("replayed")("maybe"))

            headers[request] shouldBe null
            headers[replayed] shouldBe null
        }
    }

    context("names") {
        test("refuse a name that is no header name") {
            shouldThrow<IllegalArgumentException> { HeaderName("Trace Id") }
        }

        test("render as their value") {
            HeaderName(TRACE_ID).toString() shouldBe TRACE_ID
            traceId.toString() shouldBe "HeaderKey(trace-id)"
        }
    }
    context("removing a header") {
        test("keeps the headers as they are when the header is not set") {
            val tenant = HeaderKey.text("tenant")
            val headers = headersOf(HeaderKey.text("trace-id")("t-1"))

            headers.without(tenant) shouldBe headers
        }
    }
})
