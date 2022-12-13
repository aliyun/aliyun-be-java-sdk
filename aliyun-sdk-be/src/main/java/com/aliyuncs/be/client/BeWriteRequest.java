package com.aliyuncs.be.client;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.aliyuncs.be.client.protocol.BeWriteType;
import com.google.gson.Gson;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

/**
 * @author silan.wpq
 * @date 2021/7/15
 */
@Builder
@Data
public class BeWriteRequest {
    private BeWriteType type;
    private String tableName;
    private List<Map<String, String>> contents;
    private String primaryKey;
    private String instanceName;
    private String writeMethod = "GET";

    public void validate() throws InvalidParameterException {
        if (type == null) {
            throw new InvalidParameterException("Empty write type");
        }
        if (StringUtils.isBlank(primaryKey)) {
            throw new InvalidParameterException("Primary key not set");
        }
        if (StringUtils.isBlank(tableName)) {
            throw new InvalidParameterException("Empty table name");
        }
        if (CollectionUtils.isEmpty(contents)) {
            throw new InvalidParameterException("Empty contents");
        }
        if (StringUtils.equalsIgnoreCase("POST", writeMethod) && StringUtils.isEmpty(instanceName)) {
            throw new InvalidParameterException("Empty instance name");
        }
        for (Map<String, String> content : contents) {
            if (MapUtils.isEmpty(content)) {
                throw new InvalidParameterException("Empty content");
            }
            if (!content.containsKey(primaryKey)) {
                throw new InvalidParameterException(String.format("Content should contain primary key[%s]", primaryKey));
            }
        }
    }

    private String getInstanceTableName() {
        return instanceName + "_" + tableName;
    }

    public boolean isGetWrite() {
        return StringUtils.equalsIgnoreCase("GET", writeMethod);
    }

    public String buildContent(int index) throws InvalidParameterException {
        String tableName = getInstanceTableName();
        StringBuilder kvPairBuilder = new StringBuilder();
        char separator = (char)31;

        String primaryKeyValue = StringUtils.EMPTY;
        for (Map.Entry<String, String> kvPair : contents.get(index).entrySet()) {
            if (StringUtils.isBlank(kvPair.getKey())) {
                throw new InvalidParameterException("Empty key or value for kv pair:" + new Gson().toJson(kvPair));
            }
            if (StringUtils.isBlank(kvPair.getValue())) {
                kvPair.setValue(StringUtils.EMPTY);
            }
            if (StringUtils.equals(kvPair.getKey(), primaryKey)) {
                primaryKeyValue = kvPair.getValue();
                continue;
            }
            kvPairBuilder.append(kvPair.getKey()).append("=").append(kvPair.getValue()).append(separator).append('\n');
        }

        StringBuilder contentBuilder = new StringBuilder();
        contentBuilder.append("CMD=").append(StringUtils.lowerCase(type.name())).append(separator).append('\n');
        contentBuilder.append(primaryKey).append("=").append(primaryKeyValue).append(separator).append('\n');
        contentBuilder.append(kvPairBuilder);

        String content = null;
        try {
            content = URLEncoder.encode(contentBuilder.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new InvalidParameterException(e.getMessage(), e);
        }
        String hashStr = String.valueOf(primaryKeyValue.hashCode());

        URIBuilder uriBuilder = new URIBuilder();
        uriBuilder.addParameter("table", instanceName + "_" + tableName);
        uriBuilder.addParameter("h", hashStr);
        uriBuilder.addParameter("msg", content);

        contentBuilder = new StringBuilder();
        contentBuilder.append("table=").append(tableName)
                .append("&h=").append(hashStr)
                .append("&msg=").append(content);

        return contentBuilder.toString();
    }
}
