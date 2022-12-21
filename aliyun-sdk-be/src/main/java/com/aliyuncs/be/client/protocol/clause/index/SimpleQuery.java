package com.aliyuncs.be.client.protocol.clause.index;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.google.gson.JsonObject;
import lombok.Data;

/**
 * @author silan.wpq
 * @date 2022/12/12
 */
@Data
public class SimpleQuery implements BeIndexQuery {
    private String indexName;
    private String value;

    public SimpleQuery(String indexName, String value) {
        this.indexName = indexName;
        this.value = value;
    }

    @Override
    public JsonObject buildCondition() throws InvalidParameterException {
        JsonObject matchCondition = new JsonObject();
        if (isValueQuoted()) {
            value = value.substring(1, value.length() - 1);
        }
        matchCondition.addProperty(indexName, value);
        JsonObject condition = new JsonObject();
        condition.add("match", matchCondition);
        return condition;
    }

    private boolean isValueQuoted() {
        if (value.length() <= 1) {
            return false;
        }
        if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
            return true;
        }
        if (value.charAt(0) == '"' && value.charAt(value.length() - 1) == '"') {
            return true;
        }
        return false;
    }
}
