/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.core;

import static com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase.assertWithStatement;
import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.hyper.LocalHyperTestBase;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LocalHyperTestBase.class)
public class Float4VectorTest {

    @Test
    public void testFloat4VectorQuery() {
        val expectedValues =
                IntStream.rangeClosed(0, 18).mapToObj(i -> 1.0f + (i * 0.5f)).collect(Collectors.toList());

        assertWithStatement(statement -> {
            ResultSet rs =
                    statement.executeQuery("select i::float4 from generate_series(1, 10, 0.5) g(i) order by i asc");

            List<Float> actualValues = new ArrayList<>();
            while (rs.next()) {
                actualValues.add(rs.getFloat(1));
            }

            assertThat(actualValues).containsExactlyElementsOf(expectedValues);
        });
    }
}
