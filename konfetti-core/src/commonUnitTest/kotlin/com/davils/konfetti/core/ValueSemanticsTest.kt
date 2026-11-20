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
import com.davils.konfetti.core.consume.ConsumerState
import com.davils.konfetti.core.consume.Consumption
import com.davils.konfetti.core.failure.Backoff
import com.davils.konfetti.core.failure.DeadLetter
import com.davils.konfetti.core.failure.FailurePolicy
import com.davils.konfetti.core.failure.exponential
import com.davils.konfetti.core.failure.fixed
import com.davils.konfetti.core.failure.retry
import com.davils.konfetti.core.fixture.AuditEvent
import com.davils.konfetti.core.fixture.Broken
import com.davils.konfetti.core.fixture.DeviceEvent
import com.davils.konfetti.core.fixture.checkValue
import com.davils.konfetti.core.lifecycle.BusState
import com.davils.konfetti.core.lifecycle.CloseReport
import com.davils.konfetti.core.lifecycle.DrainResult
import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.BusSnapshot
import com.davils.konfetti.core.observe.ConsumerMetrics
import com.davils.konfetti.core.observe.DropReason
import com.davils.konfetti.core.observe.SequencedObservation
import com.davils.konfetti.core.observe.TopicMetrics
import com.davils.konfetti.core.observe.TopicSnapshot
import com.davils.konfetti.core.publish.Attribution
import com.davils.konfetti.core.publish.Publication
import com.davils.konfetti.core.publish.PublishResult
import com.davils.konfetti.core.publish.Rejection
import com.davils.konfetti.core.topic.Declaration
import com.davils.konfetti.core.topic.TopicName
import com.davils.konfetti.testing.testEnvelope
import com.davils.kore.core.principal.Principal
import com.davils.kore.testing.SequentialUuidV7Generator
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldNotContain
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.time.Duration.Companion.seconds

private val sequence = SequentialUuidV7Generator()
private val first = testEnvelope(AuditEvent("first")) { identifiers = sequence }
private val second = testEnvelope(AuditEvent("second")) { identifiers = sequence }
private val alpha = ConsumerName("alpha")
private val beta = ConsumerName("beta")
private val devices = TopicName("inventory.devices")
private val audits = TopicName("audit.entries")
private val failure = Broken("secret detail")
private val other = Broken("other")

private fun metrics(received: Long = 1): ConsumerMetrics = ConsumerMetrics(
    received = received,
    delivered = 1,
    filtered = 0,
    failed = 0,
    retried = 0,
    skipped = 0,
    deadLettered = 0,
    dropped = 0,
    inFlight = 0,
    queued = 0,
)

private fun topicMetrics(published: Long = 1): TopicMetrics = TopicMetrics(
    published = published,
    rejected = 0,
    dispatched = 1,
    dropped = 0,
    queued = 0,
    consumers = 1,
    retained = 0,
)

private fun topicSnapshot(partitions: Int = 1): TopicSnapshot = TopicSnapshot(
    name = devices,
    state = BusState.Running,
    partitions = partitions,
    metrics = topicMetrics(),
    consumers = mapOf(alpha to metrics()),
)

class ValueSemanticsTest : FunSpec({
    context("observations") {
        test("compare by every field and never show a payload or a message") {
            checkValue(
                BusObservation.StateChanged(BusState.Running),
                BusObservation.StateChanged(BusState.Running),
                BusObservation.StateChanged(BusState.Closed),
            )
            checkValue(
                BusObservation.TopicDeclared(devices),
                BusObservation.TopicDeclared(devices),
                BusObservation.TopicDeclared(audits),
            )
            checkValue(BusObservation.Published(first), BusObservation.Published(first), BusObservation.Published(second))
            checkValue(
                BusObservation.Rejected(devices, Rejection.Closed),
                BusObservation.Rejected(devices, Rejection.Closed),
                BusObservation.Rejected(audits, Rejection.Closed),
                BusObservation.Rejected(devices, Rejection.Overflow),
            )
            checkValue(
                BusObservation.Delivered(first, alpha, 1, 1.seconds),
                BusObservation.Delivered(first, alpha, 1, 1.seconds),
                BusObservation.Delivered(second, alpha, 1, 1.seconds),
                BusObservation.Delivered(first, alpha, 2, 1.seconds),
            )
            checkValue(
                BusObservation.Filtered(first, alpha),
                BusObservation.Filtered(first, alpha),
                BusObservation.Filtered(second, alpha),
                BusObservation.Filtered(first, beta),
            )
            checkValue(
                BusObservation.Failed(first, alpha, 1, failure),
                BusObservation.Failed(first, alpha, 1, failure),
                BusObservation.Failed(first, alpha, 1, other),
            )
            checkValue(
                BusObservation.Retrying(first, alpha, 2, 1.seconds),
                BusObservation.Retrying(first, alpha, 2, 1.seconds),
                BusObservation.Retrying(first, alpha, 2, 2.seconds),
            )
            checkValue(
                BusObservation.Skipped(first, alpha, 1),
                BusObservation.Skipped(first, alpha, 1),
                BusObservation.Skipped(first, alpha, 2),
            )
            checkValue(
                BusObservation.DeadLettered(first, alpha, 1),
                BusObservation.DeadLettered(first, alpha, 1),
                BusObservation.DeadLettered(first, beta, 1),
            )
            checkValue(
                BusObservation.Dropped(first, alpha, DropReason.Overflow),
                BusObservation.Dropped(first, alpha, DropReason.Overflow),
                BusObservation.Dropped(first, null, DropReason.Overflow),
                BusObservation.Dropped(first, alpha, DropReason.Closed),
            )
            checkValue(
                BusObservation.ConsumerAttached(devices, alpha),
                BusObservation.ConsumerAttached(devices, alpha),
                BusObservation.ConsumerAttached(audits, alpha),
                BusObservation.ConsumerAttached(devices, beta),
            )
            checkValue(
                BusObservation.ConsumerDetached(devices, alpha),
                BusObservation.ConsumerDetached(devices, alpha),
                BusObservation.ConsumerDetached(audits, alpha),
                BusObservation.ConsumerDetached(devices, beta),
            )
            checkValue(
                BusObservation.ConsumerStopped(devices, alpha, failure),
                BusObservation.ConsumerStopped(devices, alpha, failure),
                BusObservation.ConsumerStopped(devices, alpha, other),
            )
            BusObservation.Failed(first, alpha, 1, failure).toString() shouldNotContain "secret"
            BusObservation.Dropped(first, null, DropReason.Closed).toString() shouldContain "by topic"
        }
    }

    context("metrics and snapshots") {
        test("compare by every count") {
            checkValue(metrics(), metrics(), metrics(received = 2))
            checkValue(topicMetrics(), topicMetrics(), topicMetrics(published = 2))
            checkValue(topicSnapshot(), topicSnapshot(), topicSnapshot(partitions = 2))
            checkValue(
                BusSnapshot(BusState.Running, listOf(topicSnapshot()), 0, 0),
                BusSnapshot(BusState.Running, listOf(topicSnapshot()), 0, 0),
                BusSnapshot(BusState.Closed, listOf(topicSnapshot()), 0, 0),
            )
            metrics().settled shouldBe 1
            SequencedObservation(3, 1, BusObservation.StateChanged(BusState.Closed)).toString() shouldContain "missed=1"
        }
    }

    context("results") {
        test("compare by value") {
            checkValue(PublishResult.Published(first), PublishResult.Published(first), PublishResult.Published(second))
            checkValue(
                PublishResult.Rejected(devices, Rejection.Closed),
                PublishResult.Rejected(devices, Rejection.Closed),
                PublishResult.Rejected(audits, Rejection.Closed),
                PublishResult.Rejected(devices, Rejection.Overflow),
            )
            checkValue(CloseReport(1), CloseReport(1), CloseReport(2))
            checkValue(DrainResult.TimedOut(1), DrainResult.TimedOut(1), DrainResult.TimedOut(2))
            checkValue(ConsumerState.Stopped(failure), ConsumerState.Stopped(failure), ConsumerState.Stopped(other))
            checkValue(
                Declaration.AlreadyDeclared<AuditEvent>(devices),
                Declaration.AlreadyDeclared(devices),
                Declaration.AlreadyDeclared(audits),
            )
            checkValue(Declaration.Closed<AuditEvent>(devices), Declaration.Closed(devices), Declaration.Closed(audits))
            checkValue(
                Declaration.LimitReached<AuditEvent>(devices, 1),
                Declaration.LimitReached(devices, 1),
                Declaration.LimitReached(audits, 1),
                Declaration.LimitReached(devices, 2),
            )
            checkValue(
                Consumption.Duplicate(devices, alpha),
                Consumption.Duplicate(devices, alpha),
                Consumption.Duplicate(audits, alpha),
                Consumption.Duplicate(devices, beta),
            )
            checkValue(Consumption.Closed(devices), Consumption.Closed(devices), Consumption.Closed(audits))
            checkValue(
                Consumption.LimitReached(devices, 1),
                Consumption.LimitReached(devices, 1),
                Consumption.LimitReached(audits, 1),
                Consumption.LimitReached(devices, 2),
            )
            listOf(DrainResult.Completed, DrainResult.AlreadyClosed).map(Any::toString) shouldBe
                listOf("Completed", "AlreadyClosed")
            listOf(ConsumerState.Running, ConsumerState.Draining, ConsumerState.Detached).map(Any::toString) shouldBe
                listOf("Running", "Draining", "Detached")
        }
    }

    context("policies and attributions") {
        test("compare by value") {
            checkValue(fixed(1.seconds), fixed(1.seconds), fixed(2.seconds))
            checkValue(
                exponential(1.seconds, 2.seconds),
                exponential(1.seconds, 2.seconds),
                exponential(1.seconds, 3.seconds),
                exponential(1.seconds, 2.seconds, jitter = true),
            )
            checkValue(
                retry(attempts = 2),
                retry(attempts = 2),
                retry(attempts = 3),
                retry(attempts = 2, backoff = fixed(1.seconds)),
                retry(attempts = 2, then = FailurePolicy.Skip),
            )
            checkValue(
                Attribution.forPrincipal(Principal("alice")),
                Attribution.forPrincipal(Principal("alice")),
                Attribution.forPrincipal(Principal("bob")),
            )
            listOf(FailurePolicy.Skip, FailurePolicy.Stop, FailurePolicy.DeadLetter).map(Any::toString) shouldBe
                listOf("Skip", "Stop", "DeadLetter")
            listOf(Attribution.Inherited, Attribution.Anonymous).map(Any::toString) shouldBe
                listOf("Inherited", "Anonymous")
            (fixed(1.seconds) as Backoff).toString() shouldBe "Fixed(1s)"
        }
    }

    context("events in transit") {
        test("show their type, never their content") {
            val letter = DeadLetter(original = first, consumer = alpha, attempts = 1, cause = failure)
            val publication = Publication(devices, DeviceEvent.Registered("r1"), first.headers, EmptyCoroutineContext)
            val key = TopicKey<AuditEvent>("audit.entries")

            letter.toString() shouldContain "cause=Broken"
            letter.toString() shouldNotContain "secret"
            publication.toString() shouldContain "event=Registered"
            publication.toString() shouldNotContain "r1"
            checkValue(key, TopicKey("audit.entries"), TopicKey<AuditEvent>("audit.other"))
        }
    }
})
