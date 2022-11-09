package com.aliyuncs.be.client.protocol;

import com.aliyuncs.be.client.BeReadRequest;
import com.aliyuncs.be.client.BeWriteRequest;
import com.aliyuncs.be.client.exception.InvalidParameterException;

/**
 * @author silan.wpq
 * @date 2021/7/15
 */
public interface RequestBuilder {
    String buildReadUri(String domain, int port, BeReadRequest request) throws InvalidParameterException;
    String buildWriteUri(String domain, int port, BeWriteRequest request, int index) throws InvalidParameterException;
}
