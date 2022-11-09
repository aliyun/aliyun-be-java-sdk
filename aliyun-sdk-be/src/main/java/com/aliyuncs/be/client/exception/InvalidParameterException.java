package com.aliyuncs.be.client.exception;

/**
 * @author silan.wpq
 * @date 2021/7/15
 */
public class InvalidParameterException extends BeException {
    public InvalidParameterException(String message) {
        super(message);
    }

    public InvalidParameterException(String message, Throwable cause) {
        super(message, cause);
    }
}
