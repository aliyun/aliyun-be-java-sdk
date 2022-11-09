package com.aliyuncs.be.client.protocol;

import com.aliyuncs.be.client.BeReadRequest;
import com.aliyuncs.be.client.BeWriteRequest;
import com.aliyuncs.be.client.exception.InvalidParameterException;
import org.apache.http.client.utils.URIBuilder;

/**
 * @author silan.wpq
 * @date 2021/7/15
 */
public class AgentRequestBuilder extends DefaultRequestBuilder {

    @Override
    protected URIBuilder createReadURIBuilder(String domain, int port, BeReadRequest request) throws InvalidParameterException {
        URIBuilder uriBuilder = super.createReadURIBuilder(domain, port, request);
        rewriteAgentHost(domain, port, uriBuilder);
        return uriBuilder;
    }

    @Override
    protected URIBuilder createWriteURIBuilder(String domain, int port, BeWriteRequest request, int index) throws InvalidParameterException {
        URIBuilder uriBuilder = super.createWriteURIBuilder(domain, port, request, index);
        rewriteAgentHost(domain, port, uriBuilder);
        return uriBuilder;
    }

    public void rewriteAgentHost(String domain, int port, URIBuilder uriBuilder) {
        uriBuilder.addParameter("host", domain + ":" + port);
        uriBuilder.setPort(80);
        uriBuilder.setHost("curl-proxy-cn-beijing.vpc.airec.aliyun-inc.com");
    }
}
