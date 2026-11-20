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

import com.davils.konfetti.core.header.EventHeaders

/**
 * Enriches every publication of a bus with headers before it becomes an envelope, such as with a trace id.
 *
 * Interceptors run in the order they were registered, each on the headers the one before returned. An interceptor
 * that throws is contained and counted in [com.davils.konfetti.core.observe.BusSnapshot.interceptorFailures]; the
 * publication then goes on with the headers it had before that interceptor. An interceptor runs on the publishing
 * coroutine and must not block.
 *
 * @since 1.0.0
 */
public fun interface PublishInterceptor {
    /**
     * Returns the headers [publication] should carry.
     *
     * @param publication the publication with the headers so far.
     * @return the headers to go on with, usually `publication.headers.with(…)`.
     * @since 1.0.0
     */
    public fun enrich(publication: Publication): EventHeaders
}
