package com.aliyuncs.be.client;

import lombok.Data;

/**
 * @author silan.wpq
 * @date 2021/7/15
 */
@Data
public class BeResponse<T> {
    boolean success;
    String message;
    T result;

    public BeResponse(T result) {
        this.success = true;
        this.result = result;
    }

    public BeResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public BeResponse(boolean success, String message, T result) {
        this.success = success;
        this.message = message;
        this.result = result;
    }

    public static <T> BeResponse<T> buildSuccessResponse(T result) {
        return new BeResponse<T>(result);
    }

    public static <T> BeResponse<T> buildFailureResponse(String message) {
        return new BeResponse<T>(false, message);
    }

}
