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
