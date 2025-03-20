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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.salesforce.datacloud.jdbc.exception.DataCloudJDBCException;
import lombok.experimental.UtilityClass;
import lombok.val;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;

@UtilityClass
public class MetadataResultSet {
    public static AvaticaResultSet of() throws SQLException {
        val signature = new Meta.Signature(
                ImmutableList.of(), null, ImmutableList.of(), ImmutableMap.of(), null, Meta.StatementType.SELECT);
        return of(
                null,
                new QueryState(),
                signature,
                new AvaticaResultSetMetaData(null, null, signature),
                TimeZone.getDefault(),
                null,
                ImmutableList.of());
    }

    public static AvaticaResultSet of(
            AvaticaStatement statement,
            QueryState state,
            Meta.Signature signature,
            ResultSetMetaData resultSetMetaData,
            TimeZone timeZone,
            Meta.Frame firstFrame,
            List<Object> data)
            throws SQLException {
        AvaticaResultSet result;
        try {
            result = new AvaticaResultSet(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
        } catch (SQLException e) {
            throw new DataCloudJDBCException(e);
        }
        result.execute2(new MetadataCursor(data), signature.columns);
        return result;
    }
}
