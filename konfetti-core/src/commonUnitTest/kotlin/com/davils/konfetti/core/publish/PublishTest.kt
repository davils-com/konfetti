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

package com.davils.konfetti.core.publish

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.fixture.ALICE
import com.davils.konfetti.core.fixture.AUDITOR
import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.INDEXER
import com.davils.konfetti.core.fixture.Topics
import com.davils.konfetti.core.fixture.attach
import com.davils.konfetti.core.fixture.bus
import com.davils.konfetti.core.fixture.devices
import com.davils.konfetti.core.fixture.send
import com.davils.konfetti.core.header.EventHeaders
import com.davils.konfetti.core.header.HeaderKey
import com.davils.konfetti.core.header.headersOf
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.topic.OrderingKey
import com.davils.konfetti.testing.EventRecorder
import com.davils.konfetti.testing.RecordingObserver
import com.davils.kore.core.principal.Principal
import com.davils.kore.core.principal.currentPrincipal
import com.davils.kore.core.principal.withPrincipal
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.withContext
import kotlin.coroutines.EmptyCoroutineContext

private val traceId = HeaderKey.text("trace-id")
private val tenant = HeaderKey.text("tenant")

class PublishTest : FunSpec({
    context("publishing").config(coroutineTestScope = true) {
        test("hands back the envelope it published") {
            val bus = devices()

            val envelope = bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            envelope.topic shouldBe Topics.devices.name
            envelope.payload shouldBe DeviceEvent.Registered("r1")
            envelope.correlationId shouldBe envelope.id
            envelope.causationId shouldBe null
            envelope.orderingKey shouldBe null
            bus[Topics.devices].metrics.published shouldBe 1
        }

        test("orders under the key the topic declares") {
            val bus = bus { topic(Topics.devices) { orderBy { event -> event.id } } }

            bus[Topics.devices].send(DeviceEvent.Registered("r1")).orderingKey shouldBe OrderingKey("r1")
        }

        test("carries the headers it was given") {
            val bus = devices()

            val result = bus[Topics.devices].publish(DeviceEvent.Registered("r1"), headersOf(traceId("t-1")))

            val published = result.shouldBeInstanceOf<PublishResult.Published<DeviceEvent>>()
            published.envelope.headers[traceId] shouldBe "t-1"
        }

        test("links an envelope to the one it was caused by") {
            val bus = devices()
            val topic = bus[Topics.devices]
            val cause = topic.send(DeviceEvent.Registered("r1"))

            val result = topic.publish(DeviceEvent.Renamed(id = "r1", name = "core"), causedBy = cause)

            val effect = result.shouldBeInstanceOf<PublishResult.Published<DeviceEvent>>().envelope
            effect.causationId shouldBe cause.id
            effect.correlationId shouldBe cause.correlationId
        }

        test("joins the chain of the envelope a consumer is handling") {
            val bus = bus {
                topic(Topics.devices)
                topic(Topics.audits)
            }
            val recorder = EventRecorder<AuditEvent>()
            bus[Topics.audits].attach("audit.writer") { handle(recorder::record) }
            bus[Topics.devices].attach(AUDITOR) {
                handle { envelope -> bus[Topics.audits].publish(AuditEvent("saw ${envelope.payload.id}")) }
            }

            val cause = bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            val effect = recorder.awaitCount(1).single()
            effect.causationId shouldBe cause.id
            effect.correlationId shouldBe cause.correlationId
        }
    }

    context("attribution").config(coroutineTestScope = true) {
        test("inherits the principal of the publishing coroutine") {
            val bus = devices()

            val envelope = withPrincipal(Principal(ALICE)) { bus[Topics.devices].send(DeviceEvent.Registered("r1")) }

            envelope.principal shouldBe Principal(ALICE)
        }

        test("names an explicit principal or none at all") {
            val topic = devices()[Topics.devices]

            val explicit = withPrincipal(Principal(ALICE)) {
                topic.publish(
                    DeviceEvent.Registered("r1"),
                    attribution = Attribution.forPrincipal(Principal("bob")),
                )
            }
            val anonymous = withPrincipal(Principal(ALICE)) {
                topic.publish(DeviceEvent.Registered("r2"), attribution = Attribution.Anonymous)
            }

            explicit.shouldBeInstanceOf<PublishResult.Published<DeviceEvent>>().envelope.principal shouldBe
                Principal("bob")
            anonymous.shouldBeInstanceOf<PublishResult.Published<DeviceEvent>>().envelope.principal shouldBe null
            Attribution.forPrincipal(null) shouldBe Attribution.Anonymous
        }

        test("hands the principal of an envelope to the consumer that handles it") {
            val bus = devices()
            val seen = CompletableDeferred<Principal?>()
            bus[Topics.devices].attach(INDEXER) {
                handle { seen.complete(currentPrincipal()) }
            }

            withPrincipal(Principal(ALICE)) { bus[Topics.devices].send(DeviceEvent.Registered("r1")) }

            seen.await() shouldBe Principal(ALICE)
        }
    }

    context("publishing without waiting").config(coroutineTestScope = true) {
        test("accepts while there is room") {
            val topic = devices()[Topics.devices]

            val result = topic.offer(DeviceEvent.Registered("r1"), principal = Principal(ALICE))

            result.shouldBeInstanceOf<PublishResult.Published<DeviceEvent>>().envelope.principal shouldBe
                Principal(ALICE)
        }

        test("refuses with an overflow once the partition is full") {
            val observer = RecordingObserver()
            val bus = bus {
                observe(observer)
                topic(Topics.devices) { bufferCapacity = 1 }
            }
            val topic = bus[Topics.devices]
            val first = topic.tryPublish(DeviceEvent.Registered("r1"))
            val second = topic.tryPublish(DeviceEvent.Registered("r2"))

            val third = topic.tryPublish(DeviceEvent.Registered("r3"))

            first.shouldBeInstanceOf<PublishResult.Published<DeviceEvent>>()
            second.shouldBeInstanceOf<PublishResult.Published<DeviceEvent>>()
            third shouldBe PublishResult.Rejected(Topics.devices.name, Rejection.Overflow)
            topic.metrics.rejected shouldBe 1
            observer.only<BusObservation.Rejected>().single().reason shouldBe Rejection.Overflow
        }

        test("links to an explicit cause") {
            val topic = devices()[Topics.devices]
            val cause = topic.send(DeviceEvent.Registered("r1"))

            val result = topic.tryPublish(DeviceEvent.Removed("r1"), causedBy = cause)

            result.shouldBeInstanceOf<PublishResult.Published<DeviceEvent>>().envelope.causationId shouldBe cause.id
        }
    }

    context("refusal").config(coroutineTestScope = true) {
        test("refuses every publication once the bus is closed") {
            val bus = devices()
            val topic = bus[Topics.devices]
            bus.close()

            topic.publish(DeviceEvent.Registered("r1")) shouldBe
                PublishResult.Rejected(Topics.devices.name, Rejection.Closed)
            topic.tryPublish(DeviceEvent.Registered("r2")) shouldBe
                PublishResult.Rejected(Topics.devices.name, Rejection.Closed)
        }
    }

    context("enrichment").config(coroutineTestScope = true) {
        test("lets interceptors add headers in the order they were declared") {
            val bus = devices {
                enrich { publication -> publication.headers.with(traceId("t-${publication.topic}")) }
                enrich { publication -> publication.headers.with(tenant("acme")) }
            }

            val envelope = bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            envelope.headers[traceId] shouldBe "t-inventory.devices"
            envelope.headers[tenant] shouldBe "acme"
        }

        test("hands the interceptor the event and the context of the publisher") {
            val seen = CompletableDeferred<Publication>()
            val bus = devices {
                enrich { publication ->
                    seen.complete(publication)
                    publication.headers
                }
            }

            withContext(CoroutineName(ALICE)) { bus[Topics.devices].send(DeviceEvent.Registered("r1")) }

            val publication = seen.await()
            publication.event shouldBe DeviceEvent.Registered("r1")
            publication.context[CoroutineName] shouldBe CoroutineName(ALICE)
        }

        test("gives a publisher that cannot wait an empty context") {
            val seen = CompletableDeferred<Publication>()
            val bus = devices {
                enrich { publication ->
                    seen.complete(publication)
                    publication.headers
                }
            }

            bus[Topics.devices].offer(DeviceEvent.Registered("r1"))

            seen.await().context shouldBe EmptyCoroutineContext
        }

        test("keeps the headers and counts the failure of an interceptor that throws") {
            val bus = devices {
                enrich { error("broken") }
            }

            val envelope: Envelope<DeviceEvent> = bus[Topics.devices].send(DeviceEvent.Registered("r1"))

            envelope.headers shouldBe EventHeaders.EMPTY
            bus.snapshot().interceptorFailures shouldBe 1
        }
    }
})
