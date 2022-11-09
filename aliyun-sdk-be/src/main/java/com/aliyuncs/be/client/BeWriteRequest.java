package com.aliyuncs.be.client;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.aliyuncs.be.client.protocol.BeWriteType;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

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
        for (Map<String, String> content : contents) {
            if (MapUtils.isEmpty(content)) {
                throw new InvalidParameterException("Empty content");
            }
            if (!content.containsKey(primaryKey)) {
                throw new InvalidParameterException(String.format("Content should contain primary key[%s]", primaryKey));
            }
        }
    }
}
