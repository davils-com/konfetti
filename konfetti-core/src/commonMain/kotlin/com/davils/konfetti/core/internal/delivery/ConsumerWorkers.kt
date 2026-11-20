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
import com.davils.konfetti.core.observe.DropReason
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch

internal class ConsumerWorkers<T : Event>(
    intake: ConsumerIntake,
    scope: CoroutineScope,
    private val counters: ConsumerCounters,
    private val lost: (Envelope<T>, DropReason) -> Unit,
    private val reason: () -> DropReason,
    private val deliver: suspend (Envelope<T>) -> Unit,
) {
    private val mailboxes = List(intake.concurrency) {
        Mailbox(
            capacity = intake.mailboxCapacity,
            overflow = intake.overflow,
            maximumStall = intake.maximumStall,
            lost = lost,
            reason = reason,
        )
    }
    private val preloaded = List(intake.concurrency) { mutableListOf<Envelope<T>>() }
    private val jobs = mailboxes.indices.map { index -> scope.launch(start = CoroutineStart.LAZY) { work(index) } }

    fun preload(partition: Int, envelope: Envelope<T>) {
        counters.countReceived()
        preloaded[partition % preloaded.size] += envelope
    }

    suspend fun offer(partition: Int, envelope: Envelope<T>) {
        counters.countReceived()
        mailboxes[partition % mailboxes.size].put(envelope)
    }

    fun miss(envelope: Envelope<T>) {
        counters.countReceived()
        lost(envelope, reason())
    }

    fun start() {
        jobs.forEach { job -> job.start() }
    }

    fun close() {
        mailboxes.forEach { mailbox -> mailbox.close() }
    }

    fun cancel() {
        mailboxes.forEach { mailbox -> mailbox.cancel() }
    }

    suspend fun join() {
        jobs.joinAll()
    }

    private suspend fun work(index: Int) {
        val preload = preloaded[index]
        var handed = 0
        try {
            while (handed < preload.size) {
                val envelope = preload[handed]
                handed += 1
                deliver(envelope)
            }
            mailboxes[index].consume(deliver)
        } finally {
            preload.drop(handed).forEach { envelope -> lost(envelope, reason()) }
        }
    }
}
