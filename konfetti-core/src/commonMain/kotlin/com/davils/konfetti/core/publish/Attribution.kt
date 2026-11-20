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

import com.davils.konfetti.core.internal.shown
import com.davils.kore.core.principal.Principal

private const val MAX_SHOWN_ID_LENGTH: Int = 64

/**
 * Whom a publication is attributed to: the principal an envelope carries to its consumers.
 *
 * Each consumer handles the envelope with its principal established, so
 * [com.davils.kore.core.principal.currentPrincipal] returns it inside the handler.
 *
 * @since 1.0.0
 */
public sealed interface Attribution {
    /**
     * The principal of the publishing coroutine, as [com.davils.kore.core.principal.currentPrincipal] returns it, or
     * none.
     *
     * @since 1.0.0
     */
    public object Inherited : Attribution {
        /**
         * Returns `Inherited`.
         *
         * @return the name of the attribution.
         * @since 1.0.0
         */
        override fun toString(): String = "Inherited"
    }

    /**
     * No principal, whatever the publishing coroutine carries.
     *
     * @since 1.0.0
     */
    public object Anonymous : Attribution {
        /**
         * Returns `Anonymous`.
         *
         * @return the name of the attribution.
         * @since 1.0.0
         */
        override fun toString(): String = "Anonymous"
    }

    /**
     * A principal named by the publisher.
     *
     * @property principal the principal.
     * @since 1.0.0
     */
    public class Explicit internal constructor(public val principal: Principal) : Attribution {
        /**
         * Tells whether [other] attributes to an equal principal.
         *
         * @param other the object to compare with.
         * @return true if [other] is an explicit attribution with an equal principal.
         * @since 1.0.0
         */
        override fun equals(other: Any?): Boolean = other is Explicit && principal == other.principal

        /**
         * Returns the hash of the principal.
         *
         * @return the hash.
         * @since 1.0.0
         */
        override fun hashCode(): Int = principal.hashCode()

        /**
         * Shows the attribution with the escaped id of the principal.
         *
         * @return the attribution as text.
         * @since 1.0.0
         */
        override fun toString(): String = "Explicit(${shown(principal.id, MAX_SHOWN_ID_LENGTH)})"
    }

    /**
     * Ways to make attributions.
     *
     * @since 1.0.0
     */
    public companion object {
        /**
         * Attributes to [principal], or to nobody when it is null.
         *
         * @param principal the principal, or null.
         * @return [Explicit] for a principal, [Anonymous] for null.
         * @since 1.0.0
         */
        public fun forPrincipal(principal: Principal?): Attribution {
            if (principal == null) return Anonymous
            return Explicit(principal)
        }
    }
}
