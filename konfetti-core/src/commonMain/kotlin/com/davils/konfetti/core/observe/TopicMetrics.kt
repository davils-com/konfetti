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

package com.davils.konfetti.core.observe

/**
 * The counts of one topic, read without stopping it.
 *
 * Each count is read on its own while envelopes move; once the topic is drained, `published == dispatched + dropped`
 * holds and nothing is queued.
 *
 * @property published the publications the topic accepted.
 * @property rejected the publications it rejected.
 * @property dispatched the envelopes it handed to its consumers.
 * @property dropped the envelopes it dropped before any consumer received them.
 * @property queued the accepted envelopes waiting in its partitions.
 * @property consumers the consumers attached now.
 * @property retained the envelopes it retains now.
 * @since 1.0.0
 */
public class TopicMetrics internal constructor(
    public val published: Long,
    public val rejected: Long,
    public val dispatched: Long,
    public val dropped: Long,
    public val queued: Long,
    public val consumers: Int,
    public val retained: Int,
) {
    private val fields: List<Long>
        get() = listOf(published, rejected, dispatched, dropped, queued, consumers.toLong(), retained.toLong())

    /**
     * Tells whether [other] holds the same counts.
     *
     * @param other the object to compare with.
     * @return true if [other] are equal metrics.
     * @since 1.0.0
     */
    override fun equals(other: Any?): Boolean = other is TopicMetrics && fields == other.fields

    /**
     * Returns a hash of the counts, consistent with [equals].
     *
     * @return the hash.
     * @since 1.0.0
     */
    override fun hashCode(): Int = fields.hashCode()

    /**
     * Shows every count.
     *
     * @return the metrics as text.
     * @since 1.0.0
     */
    override fun toString(): String =
        "TopicMetrics(published=$published, rejected=$rejected, dispatched=$dispatched, dropped=$dropped, " +
            "queued=$queued, consumers=$consumers, retained=$retained)"
}
