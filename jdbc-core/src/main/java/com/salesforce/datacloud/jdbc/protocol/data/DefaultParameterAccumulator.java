/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

@Getter
public class DefaultParameterAccumulator implements ParameterAccumulator {
    static final String PARAMETER_INDEX_ERROR = "Parameter index must be greater than 0";

    private final List<ParameterBinding> parameters = new ArrayList<>();

    @Override
    public void setParameter(int index, HyperType type, Object value) {
        if (index <= 0) {
            throw new IllegalArgumentException(PARAMETER_INDEX_ERROR);
        }

        while (parameters.size() < index) {
            parameters.add(null);
        }
        parameters.set(index - 1, new ParameterBinding(type, value));
    }

    @Override
    public void clearParameters() {
        parameters.clear();
    }
}
