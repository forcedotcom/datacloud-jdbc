/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import java.util.List;

/**
 * Collects {@link ParameterBinding}s by 1-based position for a prepared statement execution.
 *
 * <p>Engine-level abstraction usable independently of JDBC. The JDBC
 * {@link java.sql.PreparedStatement} layer wraps this with the
 * {@link java.sql.SQLException}-shaped API it has to expose.
 */
public interface ParameterAccumulator {
    /**
     * Binds {@code value} to the {@code index}-th parameter (1-based) with the given
     * {@link HyperType}. {@code null} values map to SQL {@code NULL}.
     *
     * @throws IllegalArgumentException if {@code index <= 0}
     */
    void setParameter(int index, HyperType type, Object value);

    void clearParameters();

    List<ParameterBinding> getParameters();
}
