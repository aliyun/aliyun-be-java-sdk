package com.aliyuncs.be.client.protocol.clause;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import lombok.Data;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * @author silan.wpq
 * @date 2021/8/2
 */
@Data
public class ScorerClause {
    String clause;

    public ScorerClause(String clause) {
        this.clause = clause;
    }

    public String buildParams() throws InvalidParameterException {
        try {
            return URLEncoder.encode(clause, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new InvalidParameterException("Failed to encode scorer clause:" + clause, e);
        }
    }

}
