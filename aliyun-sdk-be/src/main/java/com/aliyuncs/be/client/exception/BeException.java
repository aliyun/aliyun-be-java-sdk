package com.aliyuncs.be.client.exception;

/**
 * @author silan.wpq
 * @date 2021/7/15
 */
public class BeException extends Exception {
    public BeException(String message) {
        super(message);
    }

    public BeException(String message, Throwable cause) {
        super(message, cause);
    }
}
