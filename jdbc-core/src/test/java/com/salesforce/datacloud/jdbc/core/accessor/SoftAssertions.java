/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
