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

package com.davils.konfetti.core.lifecycle

/**
 * The state of a bus or one of its topics.
 *
 * A bus only moves forward: from [Running] to [Draining] to [Closed], or from [Running] straight to [Closed].
 *
 * @since 1.0.0
 */
public enum class BusState {
    /**
     * The bus takes publications, declarations and consumers.
     *
     * @since 1.0.0
     */
    Running,

    /**
     * The bus takes nothing new and hands every accepted envelope to its consumers before it closes.
     *
     * @since 1.0.0
     */
    Draining,

    /**
     * The bus takes nothing and delivers nothing; what was still on its way was dropped and accounted for.
     *
     * @since 1.0.0
     */
    Closed,
}
