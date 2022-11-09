package com.aliyuncs.be.flink;

import com.aliyuncs.be.client.BeClient;
import com.aliyuncs.be.client.BeWriteRequest;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

import java.util.Properties;

/**
 * @author silan.wpq
 * @date 2022/5/7
 */
@Slf4j
public class BeRequestSinkFunction extends AbstractBeRichSinkFunction<BeWriteRequest> {

    private final String domain;
    private final int port;
    private final String userName;
    private final String passWord;
    private final Properties properties;
    private boolean dryRun = false;


    public BeRequestSinkFunction(String domain, int port, String userName, String passWord, Properties properties) {
        if (properties == null) {
            properties = new Properties();
        }
        this.domain = domain;
        this.port = port;
        this.userName = userName;
        this.passWord = passWord;
        this.properties = properties;

        this.retryNum = Integer.parseInt(properties.getProperty(RETRY_NUM, "3"));
        this.dryRun = Boolean.parseBoolean(properties.getProperty("dryRun", "false"));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.client = new BeClient(domain, port, userName, passWord);
    }

    @Override
    public void invoke(BeWriteRequest request, Context context) throws Exception {
        if (request == null) {
            return;
        }
        if (dryRun) {
            log.info("Request:" + new Gson().toJson(request));
        } else {
            sendRequest(request);
        }
    }

}
