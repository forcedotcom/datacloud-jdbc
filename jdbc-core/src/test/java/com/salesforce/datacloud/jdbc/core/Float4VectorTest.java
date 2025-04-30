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

import static com.salesforce.datacloud.jdbc.hyper.HyperTestBase.assertWithStatement;
import static org.assertj.core.api.Assertions.assertThat;

import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HyperTestBase.class)
public class Float4VectorTest {

    @Test
    public void testFloat4VectorQuery() {
        // Expected values from generate_series(1, 10, 0.5)
        val expectedValues =
                IntStream.rangeClosed(0, 18).mapToObj(i -> 1.0f + (i * 0.5f)).collect(Collectors.toList());

        assertWithStatement(statement -> {
            ResultSet rs = statement.executeQuery("select i::real from generate_series(1, 10, 0.5) g(i)");

            List<Float> actualValues = new ArrayList<>();
            while (rs.next()) {
                actualValues.add(rs.getFloat(1));
            }

            assertThat(actualValues).containsExactlyElementsOf(expectedValues);
        });
    }
}
