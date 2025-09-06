/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
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
