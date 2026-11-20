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

import com.davils.konfetti.core.topic.TopicName

private const val MAX_LISTED_TOPICS: Int = 16

/**
 * Thrown by [com.davils.konfetti.core.EventBus.get] for a key whose topic the bus does not carry.
 *
 * @property topic the topic that was asked for.
 * @property declared the topics the bus carries, sorted.
 * @since 1.0.0
 */
public class TopicNotDeclaredException internal constructor(
    public val topic: TopicName,
    public val declared: Set<TopicName>,
) : IllegalArgumentException("Topic '$topic' is not declared on this bus, which carries ${render(declared)}")

private fun render(declared: Set<TopicName>): String {
    if (declared.isEmpty()) return "no topics"
    val listed = declared.take(MAX_LISTED_TOPICS).map { name -> "'$name'" }
    val omitted = declared.size - listed.size
    val tail = listOfNotNull("… $omitted more".takeIf { omitted > 0 })
    return (listed + tail).joinToString(prefix = "[", postfix = "]")
}
