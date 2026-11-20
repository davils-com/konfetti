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

package com.davils.konfetti.core.fixture

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event
import com.davils.konfetti.core.EventBus
import com.davils.konfetti.core.Topic
import com.davils.konfetti.core.bus.EventBusBuilder
import com.davils.konfetti.core.consume.Consumer
import com.davils.konfetti.core.consume.ConsumerBuilder
import com.davils.konfetti.core.publish.PublishResult
import com.davils.konfetti.testing.testEventBus
import io.kotest.core.test.TestScope
import io.kotest.engine.coroutines.backgroundScope
import io.kotest.engine.coroutines.testScheduler
import io.kotest.matchers.types.shouldBeInstanceOf

internal fun TestScope.bus(block: EventBusBuilder.() -> Unit = {}): EventBus = testEventBus(backgroundScope) {
    timeSource = testScheduler.timeSource
    block()
}

internal fun TestScope.devices(block: EventBusBuilder.() -> Unit = {}): EventBus = bus {
    topic(Topics.devices)
    block()
}

internal fun <T : Event> Topic<T>.attach(name: String, block: ConsumerBuilder<T>.() -> Unit): Consumer {
    val consumption = consume(name, block = block)
    return consumption.getOrThrow()
}

internal suspend fun <T : Event> Topic<T>.send(event: T): Envelope<T> {
    val result = publish(event)
    return result.shouldBeInstanceOf<PublishResult.Published<T>>().envelope
}
