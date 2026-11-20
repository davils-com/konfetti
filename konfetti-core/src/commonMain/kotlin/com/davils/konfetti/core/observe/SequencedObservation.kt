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
 * An observation of an [ObservationStream] with its place in the stream.
 *
 * @property sequence the number of the observation in the stream, starting at 1 and growing by 1 per observation.
 * @property missed the observations this collector missed right before this one because it fell behind the capacity
 * of the stream.
 * @property observation the observation.
 * @since 1.0.0
 */
public class SequencedObservation internal constructor(
    public val sequence: Long,
    public val missed: Long,
    public val observation: BusObservation,
) {
    /**
     * Shows the observation with its sequence and the observations missed before it.
     *
     * @return the observation as text.
     * @since 1.0.0
     */
    override fun toString(): String = "SequencedObservation(sequence=$sequence, missed=$missed, $observation)"
}
