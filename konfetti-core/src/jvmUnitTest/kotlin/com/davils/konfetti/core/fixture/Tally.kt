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

import com.davils.konfetti.core.observe.BusObservation
import com.davils.konfetti.core.observe.BusObserver
import java.util.concurrent.atomic.LongAdder

internal class Tally : BusObserver {
    val published = LongAdder()
    val settled = LongAdder()
    val dropped = LongAdder()

    override fun observe(observation: BusObservation) {
        when (observation) {
            is BusObservation.Published -> published.increment()

            is BusObservation.Delivered -> settled.increment()

            is BusObservation.Dropped -> dropped.increment()

            is BusObservation.Skipped,
            is BusObservation.Filtered,
            is BusObservation.DeadLettered,
            is BusObservation.StateChanged,
            is BusObservation.TopicDeclared,
            is BusObservation.Rejected,
            is BusObservation.Failed,
            is BusObservation.Retrying,
            is BusObservation.ConsumerAttached,
            is BusObservation.ConsumerStopped,
            is BusObservation.ConsumerDetached,
            -> Unit
        }
    }
}
