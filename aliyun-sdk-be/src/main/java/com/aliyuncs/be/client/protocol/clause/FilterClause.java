package com.aliyuncs.be.client.protocol.clause;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * @author silan.wpq
 * @date 2021/8/2
 */
public class FilterClause {
    BeFilter filter;
    String clause;

    public FilterClause(BeFilter filter) {
        this.filter = filter;
    }

    public FilterClause(String clause) {
        this.clause = clause;
    }

    public String buildParams() throws InvalidParameterException {
        String queryClause;
        if (StringUtils.isNotBlank(clause)) {
            queryClause = clause;
        } else {
            queryClause = filter.getConditionValue();
        }
        try {
            return URLEncoder.encode(queryClause, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new InvalidParameterException("Failed to encode queryClause:" + queryClause, e);
        }
    }

    public InFilter getInFilter() {
        if (filter == null) {
            return null;
        }
        return filter.getInFilter();
    }
}
