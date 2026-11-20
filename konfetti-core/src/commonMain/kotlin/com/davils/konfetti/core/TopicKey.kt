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

import com.davils.konfetti.core.internal.displayName
import com.davils.konfetti.core.topic.TopicName
import kotlin.reflect.KClass

private const val HASH_FACTOR: Int = 31

/**
 * The typed key of a topic: its name and the type of the events it carries.
 *
 * A key is made with the function [TopicKey] and passed to [EventBus.get] to resolve the topic with its event type;
 * a key whose type differs from the declared type fails to resolve instead of casting.
 *
 * @param T the type of the events of the topic.
 * @property name the name of the topic.
 * @property eventType the class of the events of the topic.
 * @since 1.0.0
 */
public class TopicKey<T : Event> @PublishedApi internal constructor(
    public val name: TopicName,
    public val eventType: KClass<T>,
) {
    /**
     * Tells whether [other] is a key with the same name and event type.
     *
     * @param other the object to compare with.
     * @return true if [other] is an equal key.
     * @since 1.0.0
     */
    override fun equals(other: Any?): Boolean =
        other is TopicKey<*> && name == other.name && eventType == other.eventType

    /**
     * Returns a hash of the name and the event type, consistent with [equals].
     *
     * @return the hash.
     * @since 1.0.0
     */
    override fun hashCode(): Int = HASH_FACTOR * name.hashCode() + eventType.hashCode()

    /**
     * Shows the key as `TopicKey(name: Type)`.
     *
     * @return the key as text.
     * @since 1.0.0
     */
    override fun toString(): String = "TopicKey($name: ${eventType.displayName})"
}

/**
 * Makes the key of the topic [name] that carries events of type [T], such as
 * `TopicKey<DeviceEvent>("inventory.devices")`.
 *
 * @param T the type of the events of the topic.
 * @param name the name of the topic.
 * @return the key.
 * @throws IllegalArgumentException if [name] is no [TopicName].
 * @since 1.0.0
 */
@Suppress("FunctionNaming")
public inline fun <reified T : Event> TopicKey(name: String): TopicKey<T> = TopicKey(TopicName(name), T::class)
