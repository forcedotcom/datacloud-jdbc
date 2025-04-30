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

import com.salesforce.datacloud.jdbc.core.accessor.impl.BaseIntVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.BinaryVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.BooleanVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.DateVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.DecimalVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.DoubleVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.FloatVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.LargeListVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.ListVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.TimeStampVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.TimeVectorAccessor;
import com.salesforce.datacloud.jdbc.core.accessor.impl.VarCharVectorAccessor;
import java.sql.SQLException;
import java.util.function.IntSupplier;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;

public class QueryJDBCAccessorFactory {
    @FunctionalInterface
    public interface WasNullConsumer {
        void setWasNull(boolean wasNull);
    }

    public static QueryJDBCAccessor createAccessor(
            ValueVector vector, IntSupplier getCurrentRow, QueryJDBCAccessorFactory.WasNullConsumer wasNullConsumer)
            throws SQLException {
        Types.MinorType arrowType =
                Types.getMinorTypeForArrowType(vector.getField().getType());
        if (arrowType.equals(Types.MinorType.VARCHAR)) {
            return new VarCharVectorAccessor((VarCharVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.LARGEVARCHAR)) {
            return new VarCharVectorAccessor((LargeVarCharVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.DECIMAL)) {
            return new DecimalVectorAccessor((DecimalVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.BIT)) {
            return new BooleanVectorAccessor((BitVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.FLOAT4)) {
            return new FloatVectorAccessor((Float4Vector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.FLOAT8)) {
            return new DoubleVectorAccessor((Float8Vector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TINYINT)) {
            return new BaseIntVectorAccessor((TinyIntVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.SMALLINT)) {
            return new BaseIntVectorAccessor((SmallIntVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.INT)) {
            return new BaseIntVectorAccessor((IntVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.BIGINT)) {
            return new BaseIntVectorAccessor((BigIntVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.UINT4)) {
            return new BaseIntVectorAccessor((UInt4Vector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.VARBINARY)) {
            return new BinaryVectorAccessor((VarBinaryVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.LARGEVARBINARY)) {
            return new BinaryVectorAccessor((LargeVarBinaryVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.FIXEDSIZEBINARY)) {
            return new BinaryVectorAccessor((FixedSizeBinaryVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.DATEDAY)) {
            return new DateVectorAccessor((DateDayVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.DATEMILLI)) {
            return new DateVectorAccessor((DateMilliVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMENANO)) {
            return new TimeVectorAccessor((TimeNanoVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMEMICRO)) {
            return new TimeVectorAccessor((TimeMicroVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMEMILLI)) {
            return new TimeVectorAccessor((TimeMilliVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMESEC)) {
            return new TimeVectorAccessor((TimeSecVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMESTAMPSECTZ)) {
            return new TimeStampVectorAccessor((TimeStampSecTZVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMESTAMPSEC)) {
            return new TimeStampVectorAccessor((TimeStampSecVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMESTAMPMILLITZ)) {
            return new TimeStampVectorAccessor((TimeStampMilliTZVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMESTAMPMILLI)) {
            return new TimeStampVectorAccessor((TimeStampMilliVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMESTAMPMICROTZ)) {
            return new TimeStampVectorAccessor((TimeStampMicroTZVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMESTAMPMICRO)) {
            return new TimeStampVectorAccessor((TimeStampMicroVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMESTAMPNANOTZ)) {
            return new TimeStampVectorAccessor((TimeStampNanoTZVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.TIMESTAMPNANO)) {
            return new TimeStampVectorAccessor((TimeStampNanoVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.LIST)) {
            return new ListVectorAccessor((ListVector) vector, getCurrentRow, wasNullConsumer);
        } else if (arrowType.equals(Types.MinorType.LARGELIST)) {
            return new LargeListVectorAccessor((LargeListVector) vector, getCurrentRow, wasNullConsumer);
        }

        throw new UnsupportedOperationException(
                "Unsupported vector type: " + vector.getClass().getName());
    }
}
