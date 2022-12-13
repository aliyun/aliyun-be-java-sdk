package com.aliyuncs.be.client.protocol.clause.index;

import org.apache.commons.lang3.StringUtils;

/**
 * @author silan.wpq
 * @date 2022/12/12
 */
public enum IndexQueryOperator {
    OR,
    AND;

    public static IndexQueryOperator parseOperator(String operator) {
        return IndexQueryOperator.valueOf(StringUtils.upperCase(operator));
    }
}
