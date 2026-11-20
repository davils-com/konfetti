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

package com.davils.konfetti.core.error

import com.davils.konfetti.core.internal.displayName
import com.davils.konfetti.core.topic.TopicName
import kotlin.reflect.KClass

/**
 * Thrown by [com.davils.konfetti.core.EventBus.get] for a key whose event type differs from the type the topic was
 * declared with.
 *
 * @property topic the topic.
 * @property declared the type the topic was declared with.
 * @property requested the type of the key.
 * @since 1.0.0
 */
public class TopicTypeMismatchException internal constructor(
    public val topic: TopicName,
    public val declared: KClass<*>,
    public val requested: KClass<*>,
) : IllegalArgumentException(
    "Topic '$topic' carries ${declared.displayName} but was resolved with a key expecting ${requested.displayName}",
)
