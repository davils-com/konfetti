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
 * The counts of one consumer, read without stopping it.
 *
 * Each count is read on its own while envelopes move, so a snapshot may be a moment off between counts; once the
 * consumer is drained, `received == settled` holds and nothing is in flight or queued.
 *
 * @property received the envelopes the consumer received, from its topic or from the retained envelopes.
 * @property delivered the envelopes its handler finished.
 * @property filtered the envelopes a predicate or an interceptor kept from its handler.
 * @property failed the attempts that failed.
 * @property retried the retries it scheduled.
 * @property skipped the envelopes it gave up with [com.davils.konfetti.core.failure.FailurePolicy.Skip].
 * @property deadLettered the envelopes it moved to the dead letters.
 * @property dropped the envelopes it dropped, for any [DropReason].
 * @property inFlight the envelopes its handlers work on now.
 * @property queued the envelopes it received but has neither settled nor begun.
 * @since 1.0.0
 */
public class ConsumerMetrics internal constructor(
    public val received: Long,
    public val delivered: Long,
    public val filtered: Long,
    public val failed: Long,
    public val retried: Long,
    public val skipped: Long,
    public val deadLettered: Long,
    public val dropped: Long,
    public val inFlight: Long,
    public val queued: Long,
) {
    /**
     * The envelopes the consumer is done with: delivered, filtered, skipped, dead-lettered or dropped.
     *
     * @since 1.0.0
     */
    public val settled: Long get() = delivered + filtered + skipped + deadLettered + dropped

    private val fields: List<Long>
        get() = listOf(received, delivered, filtered, failed, retried, skipped, deadLettered, dropped, inFlight, queued)

    /**
     * Tells whether [other] holds the same counts.
     *
     * @param other the object to compare with.
     * @return true if [other] are equal metrics.
     * @since 1.0.0
     */
    override fun equals(other: Any?): Boolean = other is ConsumerMetrics && fields == other.fields

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
        "ConsumerMetrics(received=$received, delivered=$delivered, filtered=$filtered, failed=$failed, " +
            "retried=$retried, skipped=$skipped, deadLettered=$deadLettered, dropped=$dropped, inFlight=$inFlight, " +
            "queued=$queued)"
}
