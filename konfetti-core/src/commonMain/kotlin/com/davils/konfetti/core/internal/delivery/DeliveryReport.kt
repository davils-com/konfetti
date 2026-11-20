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

package com.davils.konfetti.core.internal.delivery

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.consume.ConsumerName
import com.davils.konfetti.core.internal.bus.Observers
import com.davils.konfetti.core.observe.BusObservation
import kotlin.time.Duration

internal class DeliveryReport(
    private val consumer: ConsumerName,
    private val counters: ConsumerCounters,
    private val observers: Observers,
) {
    fun delivered(envelope: Envelope<Event>, attempt: Int, duration: Duration): DeliveryOutcome {
        counters.countDelivered()
        observers.observe {
            BusObservation.Delivered(
                envelope = envelope,
                consumer = consumer,
                attempt = attempt,
                duration = duration,
            )
        }
        return DeliveryOutcome.Continue
    }

    fun filtered(envelope: Envelope<Event>): DeliveryOutcome {
        counters.countFiltered()
        observers.observe { BusObservation.Filtered(envelope, consumer) }
        return DeliveryOutcome.Continue
    }

    fun failed(envelope: Envelope<Event>, attempt: Int, cause: Throwable) {
        counters.countFailed()
        observers.observe {
            BusObservation.Failed(
                envelope = envelope,
                consumer = consumer,
                attempt = attempt,
                cause = cause,
            )
        }
    }

    fun retrying(envelope: Envelope<Event>, attempt: Int, wait: Duration) {
        counters.countRetried()
        observers.observe {
            BusObservation.Retrying(
                envelope = envelope,
                consumer = consumer,
                attempt = attempt,
                delay = wait,
            )
        }
    }

    fun skipped(envelope: Envelope<Event>, attempts: Int): DeliveryOutcome {
        counters.countSkipped()
        observers.observe { BusObservation.Skipped(envelope, consumer, attempts) }
        return DeliveryOutcome.Continue
    }

    fun deadLettered(envelope: Envelope<Event>, attempts: Int): DeliveryOutcome {
        counters.countDeadLettered()
        observers.observe { BusObservation.DeadLettered(envelope, consumer, attempts) }
        return DeliveryOutcome.Continue
    }
}
