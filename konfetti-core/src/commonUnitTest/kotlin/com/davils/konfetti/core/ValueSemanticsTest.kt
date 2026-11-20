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

private const val SEQUENCE: Long = 3
private const val ATTEMPTS: Int = 2
private const val DEVICE: String = "r1"
private const val AUDITS: String = "audit.entries"

private val sequence = SequentialUuidV7Generator()
private val first = testEnvelope(AuditEvent("first")) { identifiers = sequence }
private val second = testEnvelope(AuditEvent("second")) { identifiers = sequence }
private val alpha = ConsumerName("alpha")
private val beta = ConsumerName("beta")
private val devices = TopicName("inventory.devices")
private val audits = TopicName(AUDITS)
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
                value = BusObservation.StateChanged(BusState.Running),
                equal = BusObservation.StateChanged(BusState.Running),
                different = listOf(BusObservation.StateChanged(BusState.Closed)),
            )
            checkValue(
                value = BusObservation.TopicDeclared(devices),
                equal = BusObservation.TopicDeclared(devices),
                different = listOf(BusObservation.TopicDeclared(audits)),
            )
            checkValue(
                value = BusObservation.Published(first),
                equal = BusObservation.Published(first),
                different = listOf(BusObservation.Published(second)),
            )
            checkValue(
                value = BusObservation.Rejected(devices, Rejection.Closed),
                equal = BusObservation.Rejected(devices, Rejection.Closed),
                different = listOf(
                    BusObservation.Rejected(audits, Rejection.Closed),
                    BusObservation.Rejected(devices, Rejection.Overflow),
                ),
            )
            checkValue(
                value = BusObservation.Delivered(envelope = first, consumer = alpha, attempt = 1, duration = 1.seconds),
                equal = BusObservation.Delivered(envelope = first, consumer = alpha, attempt = 1, duration = 1.seconds),
                different = listOf(
                    BusObservation.Delivered(envelope = second, consumer = alpha, attempt = 1, duration = 1.seconds),
                    BusObservation.Delivered(envelope = first, consumer = alpha, attempt = 2, duration = 1.seconds),
                ),
            )
            checkValue(
                value = BusObservation.Filtered(first, alpha),
                equal = BusObservation.Filtered(first, alpha),
                different = listOf(BusObservation.Filtered(second, alpha), BusObservation.Filtered(first, beta)),
            )
            checkValue(
                value = BusObservation.Failed(envelope = first, consumer = alpha, attempt = 1, cause = failure),
                equal = BusObservation.Failed(envelope = first, consumer = alpha, attempt = 1, cause = failure),
                different = listOf(
                    BusObservation.Failed(envelope = first, consumer = alpha, attempt = 1, cause = other),
                ),
            )
            checkValue(
                value = BusObservation.Retrying(envelope = first, consumer = alpha, attempt = 2, delay = 1.seconds),
                equal = BusObservation.Retrying(envelope = first, consumer = alpha, attempt = 2, delay = 1.seconds),
                different = listOf(
                    BusObservation.Retrying(envelope = first, consumer = alpha, attempt = 2, delay = 2.seconds),
                ),
            )
            checkValue(
                value = BusObservation.Skipped(first, alpha, 1),
                equal = BusObservation.Skipped(first, alpha, 1),
                different = listOf(BusObservation.Skipped(first, alpha, 2)),
            )
            checkValue(
                value = BusObservation.DeadLettered(first, alpha, 1),
                equal = BusObservation.DeadLettered(first, alpha, 1),
                different = listOf(BusObservation.DeadLettered(first, beta, 1)),
            )
            checkValue(
                value = BusObservation.Dropped(first, alpha, DropReason.Overflow),
                equal = BusObservation.Dropped(first, alpha, DropReason.Overflow),
                different = listOf(
                    BusObservation.Dropped(first, null, DropReason.Overflow),
                    BusObservation.Dropped(first, alpha, DropReason.Closed),
                ),
            )
            checkValue(
                value = BusObservation.ConsumerAttached(devices, alpha),
                equal = BusObservation.ConsumerAttached(devices, alpha),
                different = listOf(
                    BusObservation.ConsumerAttached(audits, alpha),
                    BusObservation.ConsumerAttached(devices, beta),
                ),
            )
            checkValue(
                value = BusObservation.ConsumerDetached(devices, alpha),
                equal = BusObservation.ConsumerDetached(devices, alpha),
                different = listOf(
                    BusObservation.ConsumerDetached(audits, alpha),
                    BusObservation.ConsumerDetached(devices, beta),
                ),
            )
            checkValue(
                value = BusObservation.ConsumerStopped(devices, alpha, failure),
                equal = BusObservation.ConsumerStopped(devices, alpha, failure),
                different = listOf(BusObservation.ConsumerStopped(devices, alpha, other)),
            )
            val failed = BusObservation.Failed(envelope = first, consumer = alpha, attempt = 1, cause = failure)
            failed.toString() shouldNotContain "secret"
            val dropped = BusObservation.Dropped(envelope = first, consumer = null, reason = DropReason.Closed)
            dropped.toString() shouldContain "by topic"
        }
    }

    context("metrics and snapshots") {
        test("compare by every count") {
            checkValue(
                value = metrics(),
                equal = metrics(),
                different = listOf(metrics(received = 2)),
            )
            checkValue(
                value = topicMetrics(),
                equal = topicMetrics(),
                different = listOf(topicMetrics(published = 2)),
            )
            checkValue(
                value = topicSnapshot(),
                equal = topicSnapshot(),
                different = listOf(topicSnapshot(partitions = 2)),
            )
            checkValue(
                value = BusSnapshot(
                    state = BusState.Running,
                    topics = listOf(topicSnapshot()),
                    observerFailures = 0,
                    interceptorFailures = 0,
                ),
                equal = BusSnapshot(
                    state = BusState.Running,
                    topics = listOf(topicSnapshot()),
                    observerFailures = 0,
                    interceptorFailures = 0,
                ),
                different = listOf(
                    BusSnapshot(
                        state = BusState.Closed,
                        topics = listOf(topicSnapshot()),
                        observerFailures = 0,
                        interceptorFailures = 0,
                    ),
                ),
            )
            metrics().settled shouldBe 1
            SequencedObservation(
                sequence = SEQUENCE,
                missed = 1,
                observation = BusObservation.StateChanged(BusState.Closed),
            ).toString() shouldContain "missed=1"
        }
    }

    context("results") {
        test("compare by value") {
            checkValue(
                value = PublishResult.Published(first),
                equal = PublishResult.Published(first),
                different = listOf(PublishResult.Published(second)),
            )
            checkValue(
                value = PublishResult.Rejected(devices, Rejection.Closed),
                equal = PublishResult.Rejected(devices, Rejection.Closed),
                different = listOf(
                    PublishResult.Rejected(audits, Rejection.Closed),
                    PublishResult.Rejected(devices, Rejection.Overflow),
                ),
            )
            checkValue(
                value = CloseReport(1),
                equal = CloseReport(1),
                different = listOf(CloseReport(2)),
            )
            checkValue(
                value = DrainResult.TimedOut(1),
                equal = DrainResult.TimedOut(1),
                different = listOf(DrainResult.TimedOut(2)),
            )
            checkValue(
                value = ConsumerState.Stopped(failure),
                equal = ConsumerState.Stopped(failure),
                different = listOf(ConsumerState.Stopped(other)),
            )
            checkValue(
                value = Declaration.AlreadyDeclared<AuditEvent>(devices),
                equal = Declaration.AlreadyDeclared(devices),
                different = listOf(Declaration.AlreadyDeclared(audits)),
            )
            checkValue(
                value = Declaration.Closed<AuditEvent>(devices),
                equal = Declaration.Closed(devices),
                different = listOf(Declaration.Closed(audits)),
            )
            checkValue(
                value = Declaration.LimitReached<AuditEvent>(devices, 1),
                equal = Declaration.LimitReached(devices, 1),
                different = listOf(Declaration.LimitReached(audits, 1), Declaration.LimitReached(devices, 2)),
            )
            checkValue(
                value = Consumption.Duplicate(devices, alpha),
                equal = Consumption.Duplicate(devices, alpha),
                different = listOf(Consumption.Duplicate(audits, alpha), Consumption.Duplicate(devices, beta)),
            )
            checkValue(
                value = Consumption.Closed(devices),
                equal = Consumption.Closed(devices),
                different = listOf(Consumption.Closed(audits)),
            )
            checkValue(
                value = Consumption.LimitReached(devices, 1),
                equal = Consumption.LimitReached(devices, 1),
                different = listOf(Consumption.LimitReached(audits, 1), Consumption.LimitReached(devices, 2)),
            )
            listOf(DrainResult.Completed, DrainResult.AlreadyClosed).map(Any::toString) shouldBe
                listOf("Completed", "AlreadyClosed")
            listOf(ConsumerState.Running, ConsumerState.Draining, ConsumerState.Detached).map(Any::toString) shouldBe
                listOf("Running", "Draining", "Detached")
        }
    }

    context("policies and attributions") {
        test("compare by value") {
            checkValue(
                value = fixed(1.seconds),
                equal = fixed(1.seconds),
                different = listOf(fixed(2.seconds)),
            )
            checkValue(
                value = exponential(initial = 1.seconds, max = 2.seconds),
                equal = exponential(initial = 1.seconds, max = 2.seconds),
                different = listOf(
                    exponential(initial = 1.seconds, max = 3.seconds),
                    exponential(initial = 1.seconds, max = 2.seconds, jitter = true),
                ),
            )
            checkValue(
                value = retry(attempts = ATTEMPTS),
                equal = retry(attempts = ATTEMPTS),
                different = listOf(
                    retry(attempts = ATTEMPTS + 1),
                    retry(attempts = ATTEMPTS, backoff = fixed(1.seconds)),
                    retry(attempts = ATTEMPTS, then = FailurePolicy.Skip),
                ),
            )
            checkValue(
                value = Attribution.forPrincipal(Principal("alice")),
                equal = Attribution.forPrincipal(Principal("alice")),
                different = listOf(Attribution.forPrincipal(Principal("bob"))),
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
            val publication = Publication(
                topic = devices,
                event = DeviceEvent.Registered(DEVICE),
                headers = first.headers,
                context = EmptyCoroutineContext,
            )
            val key = TopicKey<AuditEvent>(AUDITS)

            letter.toString() shouldContain "cause=Broken"
            letter.toString() shouldNotContain "secret"
            publication.toString() shouldContain "event=Registered"
            publication.toString() shouldNotContain DEVICE
            checkValue(
                value = key,
                equal = TopicKey(AUDITS),
                different = listOf(TopicKey<AuditEvent>("audit.other")),
            )
        }
    }
})
