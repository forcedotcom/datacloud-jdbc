/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertWithStatement;
import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import java.sql.Types;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Repro for W-24140477: unsigned oid value 4294967295 silently becomes -1. */
@ExtendWith(LocalHyperTestBase.class)
public class OidUnsignedReproTest {

    @Test
    @SneakyThrows
    public void maxUnsignedOidRoundTrips() {
        assertWithStatement(statement -> {
            val rs = statement.executeQuery("SELECT CAST('4294967295' AS oid) AS v");
            assertThat(rs.next()).isTrue();

            System.out.println("getColumnType = " + rs.getMetaData().getColumnType(1));
            System.out.println("getColumnTypeName = " + rs.getMetaData().getColumnTypeName(1));
            System.out.println(
                    "getObject = " + rs.getObject(1) + " (" + rs.getObject(1).getClass() + ")");
            System.out.println("getString = " + rs.getString(1));
            System.out.println("getLong = " + rs.getLong(1));

            assertThat(rs.getMetaData().getColumnType(1)).isEqualTo(Types.BIGINT);
            assertThat(rs.getString(1)).isEqualTo("4294967295");
            assertThat(rs.getObject(1)).isEqualTo(4294967295L);
            assertThat(rs.getLong(1)).isEqualTo(4294967295L);
        });
    }
}
