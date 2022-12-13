package com.aliyuncs.be.client.protocol.clause.filter;

/**
 * @author silan.wpq
 * @date 2021/8/2
 */
public enum FilterOperator {
    EQ("="),
    NE("!="),
    LT("<"),
    GT(">"),
    LE("<="),
    GE(">=");

    private final String operator;

    FilterOperator(String operator) {
        this.operator = operator;
    }

    public String getOperator() {
        return operator;
    }
}
