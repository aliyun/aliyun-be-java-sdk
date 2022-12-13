package com.aliyuncs.be.client.protocol.clause.index;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author silan.wpq
 * @date 2022/12/12
 */
@Data
public class CompoundQuery implements BeIndexQuery {
    private IndexQueryOperator operator;
    private List<BeIndexQuery> queryList = new ArrayList<>();

    public CompoundQuery(IndexQueryOperator operator) {
        this.operator = operator;
    }

    public CompoundQuery(IndexQueryOperator operator, List<BeIndexQuery> queryList) {
        this.operator = operator;
        this.queryList = queryList;
    }

    public void addQuery(BeIndexQuery query) {
        queryList.add(query);
    }

    @Override
    public JsonObject buildCondition() throws InvalidParameterException {
        JsonArray array = new JsonArray();
        for (BeIndexQuery query :queryList) {
            array.add(query.buildCondition());
        }
        JsonObject object = new JsonObject();
        object.add(StringUtils.lowerCase(operator.name()), array);
        return object;
    }
}
