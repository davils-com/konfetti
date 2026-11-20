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

package com.davils.konfetti.core

import com.davils.konfetti.core.consume.ConsumerName
import com.davils.konfetti.core.error.TopicNotDeclaredException
import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.header.HeaderKey
import com.davils.konfetti.core.header.headersOf
import com.davils.konfetti.core.publish.Attribution
import com.davils.konfetti.core.topic.OrderingKey
import com.davils.konfetti.core.topic.TopicName
import com.davils.konfetti.testing.testEnvelope
import com.davils.kore.core.principal.Principal
import com.davils.kore.testing.HostileFragments
import com.davils.kore.testing.isSafeToShow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import io.kotest.property.Arb
import io.kotest.property.arbitrary.element
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.map
import io.kotest.property.checkAll

private const val LISTED: Int = 16
private const val OMITTED: Int = 24
private const val SECRET: String = "hunter2"

private val hostile: Arb<String> =
    Arb.list(Arb.element(HostileFragments.all), 1..32).map { fragments -> fragments.joinToString("") }

class InjectionTest : FunSpec({
    context("text from callers") {
        test("is escaped in the text of headers and envelopes") {
            checkAll(hostile) { text ->
                val headers = headersOf(HeaderKey.text("note")(text))
                val envelope = testEnvelope(AuditEvent(text)) {
                    this.headers = headers
                    orderingKey = OrderingKey(text)
                    principal = Principal("p-1")
                }

                isSafeToShow(headers.toString()) shouldBe true
                isSafeToShow(envelope.toString()) shouldBe true
                isSafeToShow(HeaderKey.text("note")(text).toString()) shouldBe true
            }
        }

        test("is escaped in the messages of invalid names") {
            checkAll(hostile) { text ->
                val topicFailure = runCatching { TopicName(text) }.exceptionOrNull()
                val consumerFailure = runCatching { ConsumerName(text) }.exceptionOrNull()

                isSafeToShow(topicFailure?.message.orEmpty()) shouldBe true
                isSafeToShow(consumerFailure?.message.orEmpty()) shouldBe true
            }
        }

        test("never shows a sensitive value") {
            val secret = HeaderKey.text("api-token", sensitive = true)
            val headers = headersOf(secret(SECRET))

            headers.toString() shouldNotContain SECRET
            secret(SECRET).toString() shouldNotContain SECRET
            testEnvelope(AuditEvent("x")) { this.headers = headers }.toString() shouldNotContain SECRET
        }

        test("is escaped in an explicit attribution") {
            checkAll(hostile) { text ->
                val principal = runCatching { Principal(text) }.getOrNull()
                val shown = principal?.let { valid -> Attribution.forPrincipal(valid).toString() }.orEmpty()
                isSafeToShow(shown) shouldBe true
            }
        }
    }

    context("the message of an undeclared topic") {
        test("lists a bounded number of topics") {
            val declared = (0 until LISTED + OMITTED).map { index -> TopicName("topic.t$index") }.toSet()

            val message = TopicNotDeclaredException(TopicName("missing.topic"), declared).message.orEmpty()

            message shouldContain "… $OMITTED more"
            TopicNotDeclaredException(TopicName("missing.topic"), emptySet()).message.orEmpty() shouldContain
                "no topics"
        }
    }
})
