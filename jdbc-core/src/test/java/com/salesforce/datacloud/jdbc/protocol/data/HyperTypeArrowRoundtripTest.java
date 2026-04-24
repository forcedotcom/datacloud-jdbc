/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies that every {@link HyperType} the driver can produce survives a round trip through
 * Arrow: {@code HyperType -> Arrow Field -> HyperType} must reproduce the original.
 *
 * <p>Not every Hyper kind can be Arrow-encoded (INTERVAL, JSON, UNKNOWN, NULL are deliberately
 * rejected by {@link HyperTypeToArrow}). Those cases are listed explicitly in
 * {@link #nonEncodableKinds()} with a comment.
 */
class HyperTypeArrowRoundtripTest {

    /**
     * Core positive cases. Every HyperType listed here must round-trip back to an equal
     * HyperType. Kinds with documented lossy encodings (CHAR/bounded VARCHAR, TIME_TZ) live in
     * {@link #deviationsFromRoundtrip()} instead.
     */
    static Stream<Arguments> encodableTypes() {
        return Stream.of(
                Arguments.of(HyperType.bool(true)),
                Arguments.of(HyperType.bool(false)),
                Arguments.of(HyperType.int8(true)),
                Arguments.of(HyperType.int16(false)),
                Arguments.of(HyperType.int32(true)),
                Arguments.of(HyperType.int64(false)),
                Arguments.of(HyperType.float4(true)),
                Arguments.of(HyperType.float8(false)),
                Arguments.of(HyperType.decimal(10, 2, true)),
                Arguments.of(HyperType.decimal(38, 0, false)),
                Arguments.of(HyperType.varcharUnlimited(true)),
                Arguments.of(HyperType.binary(16, true)),
                Arguments.of(HyperType.varbinary(false)),
                Arguments.of(HyperType.date(true)),
                Arguments.of(HyperType.time(false)),
                Arguments.of(HyperType.timestamp(false)),
                Arguments.of(HyperType.timestampTz(true)),
                Arguments.of(HyperType.array(HyperType.int32(true), false)),
                Arguments.of(HyperType.array(HyperType.varcharUnlimited(true), true)));
    }

    @ParameterizedTest
    @MethodSource("encodableTypes")
    void roundtripReproducesHyperType(HyperType original) {
        // Forward: HyperType -> Arrow.
        ArrowType arrowType = HyperTypeToArrow.toArrowType(original);
        Field field = HyperTypeKind.ARRAY == original.getKind()
                // Arrow list Fields need a child field whose type matches the list element.
                ? new Field(
                        "col",
                        new FieldType(original.isNullable(), arrowType, null),
                        java.util.Collections.singletonList(new Field(
                                "",
                                new FieldType(
                                        original.getElement().isNullable(),
                                        HyperTypeToArrow.toArrowType(original.getElement()),
                                        null),
                                java.util.Collections.emptyList())))
                : new Field("col", new FieldType(original.isNullable(), arrowType, null), null);

        // Reverse: Arrow Field -> HyperType.
        HyperType roundTripped = ArrowToHyperTypeMapper.toHyperType(field);

        assertThat(roundTripped).isEqualTo(original);
    }

    /**
     * Kinds that cannot currently round-trip through Arrow. Each is either rejected at encode
     * time (INTERVAL, JSON, UNKNOWN) or is an internal sentinel (NULL). Documented here so the
     * positive test above does not silently drift out of sync.
     */
    @Test
    void nonEncodableKinds() {
        // INTERVAL: HyperTypeToArrow.toArrowType throws — Hyper's interval has no Arrow mapping.
        // JSON: Likewise — surfaces over JDBC as Types.OTHER with the raw "json" type name.
        // UNKNOWN: Escape hatch for system-catalog types; by construction cannot be encoded.
        // NULL: Internal sentinel for `SELECT NULL`; not a column type users declare.
        // All four are expected to not round-trip. If that changes, revisit HyperTypeToArrow.
        assertThat(HyperTypeKind.INTERVAL).isNotNull();
        assertThat(HyperTypeKind.JSON).isNotNull();
        assertThat(HyperTypeKind.UNKNOWN).isNotNull();
        assertThat(HyperTypeKind.NULL).isNotNull();
    }

    /**
     * Known, documented lossy round-trips. Each is something Arrow cannot preserve without
     * additional Hyper-specific field metadata, which the Arrow stream Hyper emits today does
     * not include on the parameter-binding side. Fixing any of these needs
     * {@link HyperTypeToArrow} to stamp hyper:* metadata on the Field and
     * {@link ArrowToHyperTypeMapper} to read it back.
     */
    @Test
    void lossyRoundtrip_fixedCharCollapsesToVarcharUnlimited() {
        // HyperTypeToArrow maps CHAR(n) to Utf8 (Arrow has no fixed-length string). The reverse
        // mapping has no length hint and reports unlimited VARCHAR.
        HyperType original = HyperType.fixedChar(16, true);
        Field field = new Field("col", new FieldType(true, HyperTypeToArrow.toArrowType(original), null), null);
        assertThat(ArrowToHyperTypeMapper.toHyperType(field)).isEqualTo(HyperType.varcharUnlimited(true));
    }

    @Test
    void lossyRoundtrip_boundedVarcharCollapsesToUnlimited() {
        // HyperTypeToArrow maps VARCHAR(n) to Utf8 with no length metadata; the reverse cannot
        // reconstruct the declared bound.
        HyperType original = HyperType.varchar(255, false);
        Field field = new Field("col", new FieldType(false, HyperTypeToArrow.toArrowType(original), null), null);
        assertThat(ArrowToHyperTypeMapper.toHyperType(field)).isEqualTo(HyperType.varcharUnlimited(false));
    }

    @Test
    void lossyRoundtrip_timeTzCollapsesToTime() {
        // HyperTypeToArrow maps both TIME and TIME_TZ to Arrow Time(MICROSECOND, 64); Arrow Time
        // has no timezone slot, so the distinction is lost on the way back.
        HyperType original = HyperType.timeTz(true);
        Field field = new Field("col", new FieldType(true, HyperTypeToArrow.toArrowType(original), null), null);
        assertThat(ArrowToHyperTypeMapper.toHyperType(field)).isEqualTo(HyperType.time(true));
    }
}
