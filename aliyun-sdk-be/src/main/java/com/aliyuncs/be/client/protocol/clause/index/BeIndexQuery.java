package com.aliyuncs.be.client.protocol.clause.index;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.google.gson.JsonObject;

/**
 * @author silan.wpq
 * @date 2022/12/12
 */
public interface BeIndexQuery {
    JsonObject buildCondition() throws InvalidParameterException;
}
