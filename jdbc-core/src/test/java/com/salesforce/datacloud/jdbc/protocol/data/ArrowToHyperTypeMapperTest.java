/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import lombok.val;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

/**
 * Pin the {@link ArrowToHyperTypeMapper#toColumnMetadata(Field)} contract around the
 * {@link HyperTypeToArrow#JDBC_TYPE_NAME_METADATA_KEY} field-metadata override.
 *
 * <p>Two paths must work:
 * <ul>
 *   <li>An Arrow field that <i>does</i> stamp the override (the metadata path) returns a
 *       {@code ColumnMetadata} whose {@code typeName} matches the override exactly.
 *   <li>An Arrow field that does <i>not</i> stamp the override (every real-Hyper query stream)
 *       returns a {@code ColumnMetadata} whose {@code typeName} is {@code null}, so the JDBC
 *       layer falls back to the type-derived default. The fallback is implicit in the rest of
 *       the test suite — every functional test against local Hyper goes through this code
 *       path — but no assertion pinned it. This test does.
 * </ul>
 */
class ArrowToHyperTypeMapperTest {

    @Test
    void typeNameOverrideIsPickedUpWhenStamped() {
        val metadata = Collections.singletonMap(HyperTypeToArrow.JDBC_TYPE_NAME_METADATA_KEY, "TEXT");
        val field = new Field("c", new FieldType(true, new ArrowType.Utf8(), null, metadata), null);

        val column = ArrowToHyperTypeMapper.toColumnMetadata(field);

        assertThat(column.getName()).isEqualTo("c");
        assertThat(column.getType()).isEqualTo(HyperType.varcharUnlimited(true));
        assertThat(column.getTypeName()).isEqualTo("TEXT");
    }

    @Test
    void typeNameOverrideIsNullWhenAbsent() {
        // Mirrors what a real Hyper Arrow stream looks like: no datacloud-jdbc:type_name key.
        val field = new Field("c", new FieldType(true, new ArrowType.Utf8(), null), null);

        val column = ArrowToHyperTypeMapper.toColumnMetadata(field);

        assertThat(column.getName()).isEqualTo("c");
        assertThat(column.getType()).isEqualTo(HyperType.varcharUnlimited(true));
        // Null override means the JDBC layer falls back to HyperType-derived "VARCHAR".
        assertThat(column.getTypeName()).isNull();
    }

    @Test
    void typeNameOverrideIsNullWhenMetadataIsEmptyButPresent() {
        val field =
                new Field("c", new FieldType(true, new ArrowType.Int(32, true), null, Collections.emptyMap()), null);

        val column = ArrowToHyperTypeMapper.toColumnMetadata(field);

        assertThat(column.getTypeName()).isNull();
    }
}
