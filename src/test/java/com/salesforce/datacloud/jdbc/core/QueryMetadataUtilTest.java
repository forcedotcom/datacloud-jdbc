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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.salesforce.datacloud.jdbc.auth.AuthenticationSettings;
import com.salesforce.datacloud.jdbc.auth.DataCloudToken;
import com.salesforce.datacloud.jdbc.auth.TokenProcessor;
import java.util.Optional;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QueryMetadataUtilTest {
    @Mock
    private TokenProcessor tokenProcessor;

    @Mock
    private DataCloudToken dataCloudToken;

    @Mock
    private AuthenticationSettings authenticationSettings;

    @Test
    @SneakyThrows
    void excludesDataspaceWhenNull() {
        val tenantId = UUID.randomUUID().toString();

        when(dataCloudToken.getTenantId()).thenReturn(tenantId);
        when(authenticationSettings.getDataspace()).thenReturn(null);
        when(tokenProcessor.getSettings()).thenReturn(authenticationSettings);
        when(tokenProcessor.getDataCloudToken()).thenReturn(dataCloudToken);

        val actual = QueryMetadataUtil.getLakehouse(Optional.of(tokenProcessor));
        val expected = ImmutableList.of(ImmutableList.of("lakehouse:" + tenantId + ";"));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    @SneakyThrows
    void includesDataspaceWhenNotNull() {
        val tenantId = UUID.randomUUID().toString();
        val dataspace = UUID.randomUUID().toString();

        when(dataCloudToken.getTenantId()).thenReturn(tenantId);
        when(authenticationSettings.getDataspace()).thenReturn(dataspace);
        when(tokenProcessor.getSettings()).thenReturn(authenticationSettings);
        when(tokenProcessor.getDataCloudToken()).thenReturn(dataCloudToken);

        val actual = QueryMetadataUtil.getLakehouse(Optional.of(tokenProcessor));
        val expected = ImmutableList.of(ImmutableList.of("lakehouse:" + tenantId + ";" + dataspace));

        assertThat(actual).isEqualTo(expected);
    }
}
