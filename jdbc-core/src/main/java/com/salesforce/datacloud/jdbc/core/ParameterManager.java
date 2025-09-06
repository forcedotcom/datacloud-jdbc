/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import com.salesforce.datacloud.jdbc.core.model.ParameterBinding;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

interface ParameterManager {
    void setParameter(int index, int sqlType, Object value) throws SQLException;

    void clearParameters();

    List<ParameterBinding> getParameters();
}

@Getter
class DefaultParameterManager implements ParameterManager {
    private final List<ParameterBinding> parameters = new ArrayList<>();
    protected final String PARAMETER_INDEX_ERROR = "Parameter index must be greater than 0";

    @Override
    public void setParameter(int index, int sqlType, Object value) throws SQLException {
        if (index <= 0) {
            throw new DataCloudJDBCException(PARAMETER_INDEX_ERROR);
        }

        while (parameters.size() < index) {
            parameters.add(null);
        }
        parameters.set(index - 1, new ParameterBinding(sqlType, value));
    }

    @Override
    public void clearParameters() {
        parameters.clear();
    }
}
