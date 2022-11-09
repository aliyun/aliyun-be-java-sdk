package com.aliyuncs.be.client.protocol.clause;

import com.aliyuncs.be.client.exception.InvalidParameterException;

/**
 * @author silan.wpq
 * @date 2021/8/2
 */
public interface BeFilter {
    String getConditionValue() throws InvalidParameterException;
    InFilter getInFilter();
}
