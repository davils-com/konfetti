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

import com.davils.konfetti.core.Event
import com.davils.konfetti.core.header.EventHeaders
import com.davils.konfetti.core.internal.displayName
import com.davils.konfetti.core.topic.TopicName
import kotlin.coroutines.CoroutineContext

/**
 * A publication on its way to become an envelope, as a [PublishInterceptor] sees it.
 *
 * @property topic the topic the event is published on.
 * @property event the event.
 * @property headers the headers so far: those of the publisher and of the interceptors before this one.
 * @property context the coroutine context of the publisher, or the empty context for
 * [com.davils.konfetti.core.Topic.offer], which publishes outside a coroutine.
 * @since 1.0.0
 */
public class Publication internal constructor(
    public val topic: TopicName,
    public val event: Event,
    public val headers: EventHeaders,
    public val context: CoroutineContext,
) {
    /**
     * Shows the publication without its event: the topic, the headers and the type of the event.
     *
     * @return the publication as text.
     * @since 1.0.0
     */
    override fun toString(): String = "Publication(topic=$topic, headers=$headers, event=${event::class.displayName})"
}
