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

package com.davils.konfetti.core.consume

import com.davils.konfetti.core.Envelope
import com.davils.konfetti.core.Event

/**
 * Wraps every attempt of every consumer of a bus, such as to measure it or to establish a context.
 *
 * Interceptors nest in the order they were registered, the first one outermost. An interceptor calls [intercept]'s
 * `proceed` at most once to go on to the next interceptor and finally to the predicates and the handler; one that does
 * not call it keeps the envelope from the handler, which counts as filtered. A second call of `proceed` fails the
 * attempt instead of running the handler twice. A failure of an interceptor fails the attempt as a failure of the
 * handler would.
 *
 * @since 1.0.0
 */
public fun interface ConsumeInterceptor {
    /**
     * Wraps one attempt.
     *
     * @param envelope the envelope of the attempt.
     * @param consumer the consumer of the attempt.
     * @param proceed goes on with the attempt; call it at most once.
     * @since 1.0.0
     */
    public suspend fun intercept(envelope: Envelope<Event>, consumer: ConsumerName, proceed: suspend () -> Unit)
}
