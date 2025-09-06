/**
 * This file is part of https://github.com/forcedotcom/datacloud-jdbc which is released under the
 * Apache 2.0 license. See https://github.com/forcedotcom/datacloud-jdbc/blob/main/LICENSE.txt
 */
package com.salesforce.datacloud.jdbc.soql;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
class DataspaceResponse {
    List<DataSpaceAttributes> records;
    Integer totalSize;
    Boolean done;

    @Data
    public static class DataSpaceAttributes {
        Map<String, Object> attributes;

        @JsonAlias({"Name"})
        String name;
    }
}
