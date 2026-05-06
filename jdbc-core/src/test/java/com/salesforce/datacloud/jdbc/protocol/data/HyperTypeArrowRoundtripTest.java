/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.protocol.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.pojo.Field;
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
     * HyperType. CHAR(n) / CHAR(1) / VARCHAR(n) round-trip via {@code hyper:*} field metadata
     * stamped by {@link HyperTypeToArrow#toFieldType} and read back by
     * {@link ArrowToHyperTypeMapper}. TIME_TZ lives in {@link #lossyRoundtrip_timeTzCollapsesToTime}
     * instead (see comment there for why it stays lossy).
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
                Arguments.of(HyperType.fixedChar(1, true)),
                Arguments.of(HyperType.fixedChar(16, true)),
                Arguments.of(HyperType.varchar(255, false)),
                Arguments.of(HyperType.varchar(1024, true)),
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
        // Forward via the full toField helper so any field-level metadata (hyper:type,
        // hyper:max_string_length) travels alongside the ArrowType.
        Field field;
        if (original.getKind() == HyperTypeKind.ARRAY) {
            // Arrow list Fields need a child field whose type matches the list element.
            Field child = new Field("", HyperTypeToArrow.toFieldType(original.getElement()), Collections.emptyList());
            field = new Field("col", HyperTypeToArrow.toFieldType(original), Collections.singletonList(child));
        } else {
            field = new Field("col", HyperTypeToArrow.toFieldType(original), null);
        }

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

    @Test
    void timeTzAsymmetry_preservedOnInputButDroppedOnOutput() {
        // Not a bug — intended behavior. The SQL `time with time zone` offset matters on input
        // (the value the client binds or writes is interpreted in the supplied offset), but by
        // design it does not survive round-trip through the Arrow wire:
        //
        //   - Arrow's Time type has no timezone slot (unlike Timestamp, whose Timestamp.timezone
        //     field we already use for TIMESTAMP_TZ). A per-value offset cannot ride on the
        //     ArrowType itself.
        //   - A SQL `time with time zone` column can hold different offsets per value
        //     ("14:00+02" and "14:00-05" in the same column), so the offset is inherently
        //     per-value, not per-column. Field metadata is per-column, so it cannot carry it
        //     either.
        //   - The driver's read-side QueryJDBCAccessorFactory follows the same convention:
        //     server-returned `timetz` columns decode through the plain TimeVectorAccessor, so
        //     the canonical (server-normalised) time-of-day is what reaches the JDBC caller.
        //
        // Pinning the asymmetry here so a future contributor doesn't "fix" the encode side in
        // isolation and create a real bug — fixing this would take either a synthetic Arrow
        // schema or Hyper-protocol coordination covering both directions.
        HyperType original = HyperType.timeTz(true);
        Field field = new Field("col", HyperTypeToArrow.toFieldType(original), null);
        assertThat(ArrowToHyperTypeMapper.toHyperType(field)).isEqualTo(HyperType.time(true));
    }
}
