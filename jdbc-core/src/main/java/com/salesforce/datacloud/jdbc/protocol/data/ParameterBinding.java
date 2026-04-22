/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A single prepared-statement parameter, consisting of the {@link HyperType} the driver will bind
 * the parameter as and the Java {@link Object} value (may be {@code null} for a SQL {@code NULL}).
 */
@AllArgsConstructor
@Getter
public class ParameterBinding {
    private final HyperType type;
    private final Object value;
}
