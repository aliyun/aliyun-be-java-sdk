package com.aliyuncs.be.client.protocol.clause.exposure;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

@Data
public class ExposureClause {
    private String name;
    private List<String> values;

    public ExposureClause(String name, List<String> values) {
        this.name = name;
        this.values = values;
    }

    public ExposureClause(List<String> values) {
        this("user_id", values);
    }

    public void validate() throws InvalidParameterException {
        if (StringUtils.isBlank(name)) {
            throw new InvalidParameterException("Empty exposure name");
        }
    }

    public String flatValues() {
        if (CollectionUtils.isEmpty(values)) {
            return StringUtils.EMPTY;
        }
        return StringUtils.join(values, ',');
    }
}
