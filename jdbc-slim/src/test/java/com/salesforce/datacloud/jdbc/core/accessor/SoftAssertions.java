/*
 * Copyright (c) 2024, Salesforce, Inc.
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
package com.salesforce.datacloud.jdbc.core.accessor;

/** Entry point for soft assertions of different data types. */
@javax.annotation.Generated(value = "assertj-assertions-generator")
public class SoftAssertions extends org.assertj.core.api.SoftAssertions {

    /**
     * Creates a new "soft" instance of <code>
     * {@link com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorAssert}</code>.
     *
     * @param actual the actual value.
     * @return the created "soft" assertion object.
     */
    @org.assertj.core.util.CheckReturnValue
    public com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorAssert assertThat(
            com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor actual) {
        return proxy(
                com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessorAssert.class,
                com.salesforce.datacloud.jdbc.core.accessor.QueryJDBCAccessor.class,
                actual);
    }
}
