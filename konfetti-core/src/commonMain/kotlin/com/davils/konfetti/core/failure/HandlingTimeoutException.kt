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

package com.davils.konfetti.core.failure

import com.davils.konfetti.core.consume.ConsumerName
import kotlin.time.Duration

/**
 * The failure of an attempt whose handler did not finish within the handling timeout of its consumer.
 *
 * The handler is cancelled when the timeout passes; the failure policy of the consumer then decides as for any other
 * failure.
 *
 * @property consumer the consumer whose handler timed out.
 * @property timeout the timeout that passed.
 * @since 1.0.0
 */
public class HandlingTimeoutException internal constructor(
    public val consumer: ConsumerName,
    public val timeout: Duration,
) : RuntimeException("Consumer '$consumer' did not finish handling an envelope within $timeout")
