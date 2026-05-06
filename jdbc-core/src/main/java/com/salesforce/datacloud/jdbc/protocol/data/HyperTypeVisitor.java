/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

/**
 * Exhaustive visitor over {@link HyperTypeKind}. Callers that must handle every kind should
 * implement this interface; the compiler will then surface any newly added kind as a missing
 * method. Prefer this over open-coded {@code switch} statements where exhaustiveness matters.
 */
public interface HyperTypeVisitor<T> {
    T visitBool(HyperType t);

    T visitInt8(HyperType t);

    T visitInt16(HyperType t);

    T visitInt32(HyperType t);

    T visitInt64(HyperType t);

    T visitOid(HyperType t);

    T visitFloat4(HyperType t);

    T visitFloat8(HyperType t);

    T visitDecimal(HyperType t);

    T visitChar(HyperType t);

    T visitVarchar(HyperType t);

    T visitBinary(HyperType t);

    T visitVarbinary(HyperType t);

    T visitDate(HyperType t);

    T visitTime(HyperType t);

    T visitTimeTz(HyperType t);

    T visitTimestamp(HyperType t);

    T visitTimestampTz(HyperType t);

    T visitArray(HyperType t);

    T visitNull(HyperType t);

    T visitInterval(HyperType t);

    T visitJson(HyperType t);

    T visitUnknown(HyperType t);
}
