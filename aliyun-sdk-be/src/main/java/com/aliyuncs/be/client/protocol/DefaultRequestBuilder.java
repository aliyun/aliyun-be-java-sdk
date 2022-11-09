package com.aliyuncs.be.client.protocol;

import com.aliyuncs.be.client.BeReadRequest;
import com.aliyuncs.be.client.BeWriteRequest;
import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Map;

/**
 * @author silan.wpq
 * @date 2021/7/15
 */
public class DefaultRequestBuilder implements RequestBuilder, Serializable {

    @Override
    public String buildReadUri(String domain, int port, BeReadRequest request)
            throws InvalidParameterException {
        request.validate();
        URIBuilder uriBuilder = createReadURIBuilder(domain, port, request);
        String queryStr = uriBuilder.toString();
        try {
            return URLDecoder.decode(queryStr, "UTF-8");
        } catch (Exception e) {
            throw new InvalidParameterException(e.getMessage(), e);
        }
    }

    @Override
    public String buildWriteUri(String domain, int port, BeWriteRequest request, int index)
            throws InvalidParameterException {
        request.validate();
        URIBuilder uriBuilder = createWriteURIBuilder(domain, port, request, index);
        String writeStr = uriBuilder.toString();
        try {
            return URLDecoder.decode(writeStr, "UTF-8");
        } catch (Exception e) {
            throw new InvalidParameterException(e.getMessage(), e);
        }
    }

    protected URIBuilder createWriteURIBuilder(String domain, int port, BeWriteRequest request, int index)
            throws InvalidParameterException {
        URIBuilder uriBuilder = new URIBuilder();
        uriBuilder.setHost(domain);
        uriBuilder.setPort(port);
        uriBuilder.setPath("sendmsg");
        uriBuilder.setScheme("http");

        StringBuilder kvPairBuilder = new StringBuilder();
        char separator = (char)31;

        String primaryKeyValue = StringUtils.EMPTY;
        for (Map.Entry<String, String> kvPair : request.getContents().get(index).entrySet()) {
            if (StringUtils.isBlank(kvPair.getKey())) {
                throw new InvalidParameterException("Empty key or value for kv pair:" + new Gson().toJson(kvPair));
            }
            if (StringUtils.isBlank(kvPair.getValue())) {
                kvPair.setValue(StringUtils.EMPTY);
            }
            if (StringUtils.equals(kvPair.getKey(), request.getPrimaryKey())) {
                primaryKeyValue = kvPair.getValue();
                continue;
            }
            kvPairBuilder.append(kvPair.getKey()).append("=").append(kvPair.getValue()).append(separator).append('\n');
        }

        StringBuilder contentBuilder = new StringBuilder();
        contentBuilder.append("CMD=").append(StringUtils.lowerCase(request.getType().name())).append(separator).append('\n');
        contentBuilder.append(request.getPrimaryKey()).append("=").append(primaryKeyValue).append(separator).append('\n');
        contentBuilder.append(kvPairBuilder);

        String content = null;
        try {
            content = URLEncoder.encode(contentBuilder.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new InvalidParameterException(e.getMessage(), e);
        }
        String hashStr = String.valueOf(primaryKeyValue.hashCode());

        uriBuilder.addParameter("table", request.getTableName());
        uriBuilder.addParameter("h", hashStr);
        uriBuilder.addParameter("msg", content);
        return uriBuilder;
    }

    protected URIBuilder createReadURIBuilder(String domain, int port, BeReadRequest request) throws InvalidParameterException {
        URIBuilder uriBuilder = new URIBuilder();
        uriBuilder.setHost(domain);
        uriBuilder.setPort(port);
        uriBuilder.setPath("/be");
        uriBuilder.setScheme("http");
        addParams(uriBuilder, request);
        return uriBuilder;
    }

    protected void addParams(URIBuilder uriBuilder, BeReadRequest request) throws InvalidParameterException {
        uriBuilder.addParameter("biz_name", "searcher");
        uriBuilder.addParameter("p", request.getBizName());
        uriBuilder.addParameter("s", request.getBizName());
        uriBuilder.addParameter("return_count", String.valueOf(request.getReturnCount()));
        uriBuilder.addParameter("outfmt", "json2");
        if (CollectionUtils.isNotEmpty(request.getRecallParams())) {
            for (RecallParam param : request.getRecallParams()) {
                uriBuilder.addParameter(param.getTriggerKey(), param.flatTriggerItems());
                if (param.getScorerClause() != null) {
                    uriBuilder.addParameter(param.getScorerKey(), param.getScorerClause().buildParams());
                }
                if (StringUtils.isNotBlank(param.getRecallName())) {
                    uriBuilder.addParameter(param.getReturnCountKey(), String.valueOf(param.getReturnCount()));
                }
            }
        }
        if (request.getFilterClause() != null && StringUtils.isNotBlank(request.getFilterClause().buildParams())) {
            uriBuilder.addParameter("filter_rule", request.getFilterClause().buildParams());
            if (request.getFilterClause().getInFilter() != null) {
                request.getFilterClause().getInFilter().addInFilterParams(uriBuilder);
            }
        }
        if (request.getExposureClause() != null && StringUtils.isNotBlank(request.getExposureClause().flatValues())) {
            uriBuilder.addParameter(request.getExposureClause().getName(), request.getExposureClause().flatValues());
        }
        if (request.getQueryParams() != null) {
            for (Map.Entry<String, String> entry : request.getQueryParams().entrySet()) {
                uriBuilder.addParameter(entry.getKey(), entry.getValue());
            }
        }
    }
}
