package com.aliyuncs.be.client.protocol.clause;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;

import java.util.List;

/**
 * @author silan.wpq
 * @date 2021/8/2
 */
public class InFilter implements BeFilter {

    public InFilter(String fieldName, List<String> fieldValues) {
        this.fieldName = fieldName;
        this.fieldValues = fieldValues;
    }

    private String fieldName;
    private List<String> fieldValues;

    @Override
    public String getConditionValue() {
        return "in_filter()";
    }

    @Override
    public InFilter getInFilter() {
        return this;
    }

    public void addInFilterParams(URIBuilder uriBuilder) {
        uriBuilder.addParameter("in_filter_field", fieldName);
        uriBuilder.addParameter("in_filter_values", StringUtils.join(fieldValues, ','));
    }
}
