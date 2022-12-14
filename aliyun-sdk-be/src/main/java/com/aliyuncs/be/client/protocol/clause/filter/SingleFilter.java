package com.aliyuncs.be.client.protocol.clause.filter;

import com.aliyuncs.be.client.protocol.clause.filter.BeFilter;
import com.aliyuncs.be.client.protocol.clause.filter.FilterOperator;
import com.aliyuncs.be.client.protocol.clause.filter.InFilter;

/**
 * @author silan.wpq
 * @date 2021/8/2
 */
public class SingleFilter implements BeFilter {
    private String left;
    private String right;
    private FilterOperator operator;

    public SingleFilter(String left, FilterOperator operator, String right) {
        this.left = left;
        this.right = right;
        this.operator = operator;
    }

    @Override
    public String getConditionValue() {
        return left + operator.getOperator() + right;
    }

    @Override
    public InFilter getInFilter() {
        return null;
    }
}
