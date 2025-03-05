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
package com.salesforce.datacloud.jdbc.auth;

interface TokenCache {
    void setDataCloudToken(DataCloudToken dataCloudToken);

    void clearDataCloudToken();

    DataCloudToken getDataCloudToken();
}

class TokenCacheImpl implements TokenCache {
    private DataCloudToken dataCloudToken;

    @Override
    public void setDataCloudToken(DataCloudToken dataCloudToken) {
        this.dataCloudToken = dataCloudToken;
    }

    @Override
    public void clearDataCloudToken() {
        this.dataCloudToken = null;
    }

    @Override
    public DataCloudToken getDataCloudToken() {
        return this.dataCloudToken;
    }
}
