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

plugins {
    id("org.jetbrains.kotlin.multiplatform")
    id("konfetti.base")
    id("konfetti.dependency-locking")
    id("konfetti.git-hooks")
    id("konfetti.trivy")
    id("konfetti.assurance")
    id("konfetti.platform")
    id("konfetti.web-tools")
    id("konfetti.testing")
    id("konfetti.detekt")
    id("konfetti.coverage")
    id("konfetti.api-validation")
    id("konfetti.docs")
    id("konfetti.publish")
    id("konfetti.archives")
    id("konfetti.tool-classpaths")
}

kreate {
    assurance {
        advisories {
            configurations = setOf(
                "jvmCompileClasspath",
                "jvmRuntimeClasspath",
                "jvmUnitTestCompileClasspath",
                "jvmUnitTestRuntimeClasspath",
                "wasmJsCompileClasspath",
                "wasmJsRuntimeClasspath",
                "wasmJsTestCompileClasspath",
                "wasmJsTestRuntimeClasspath",
            )
        }
    }
}
