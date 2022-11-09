package com.aliyuncs.be.flink;

import com.aliyuncs.be.client.BeClient;
import com.aliyuncs.be.client.BeResponse;
import com.aliyuncs.be.client.BeWriteRequest;
import com.aliyuncs.be.client.exception.InvalidParameterException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author silan.wpq
 * @date 2022/9/26
 */
@Slf4j
public class AbstractBeRichSinkFunction<IN> extends RichSinkFunction<IN> {

    public static final String RETRY_NUM = "retryNum";
    protected transient BeClient client;
    protected int retryNum = 3;

    protected void sendRequest(BeWriteRequest request) {
        int currentRetry = 0;
        BeResponse resp = null;
        do {
            try {
                // TODO support batch write
                resp = client.write(request);
            } catch (InvalidParameterException e) {
                log.error("Invalid request, message:" + e.getMessage(), e);
                return;
            }
            if (resp.isSuccess()) {
                return;
            }
            log.debug("Illegal resp for {} request, continue to retry, resp[{}]", currentRetry, resp);
            currentRetry++;
        } while (retryNum > 0 && currentRetry <= retryNum);
        throw new RuntimeException(String.format("Request failed for %s request[%s], resp[%s]", retryNum, request, resp));
    }

}
