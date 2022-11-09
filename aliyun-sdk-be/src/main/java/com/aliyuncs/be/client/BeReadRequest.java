package com.aliyuncs.be.client;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.aliyuncs.be.client.protocol.RecallParam;
import com.aliyuncs.be.client.protocol.clause.ExposureClause;
import com.aliyuncs.be.client.protocol.clause.FilterClause;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author silan.wpq
 * @date 2021/7/15
 */
@Builder
@Data
public class BeReadRequest {
    private int returnCount = 10;
    private String bizName;
    private List<RecallParam> recallParams;
    private ExposureClause exposureClause;
    private FilterClause filterClause;
    private Map<String, String> queryParams;
    private boolean rawQuery = false;

    public void validate() throws InvalidParameterException {
        if (returnCount < 0) {
            throw new InvalidParameterException(String.format("Return count[%s] should be greater than 0", returnCount));
        }
        if (StringUtils.isBlank(bizName)) {
            throw new InvalidParameterException("Empty biz name");
        }
        if (isRawQuery()) {
            return;
        }
        if (CollectionUtils.isEmpty(recallParams)) {
            throw new InvalidParameterException("Recall params is empty");
        }
        Set<String> recallNames = new HashSet<>();
        for (RecallParam param : recallParams) {
            param.validate();
            String name = StringUtils.isEmpty(param.getRecallName()) ? "" : param.getRecallName();
            if (recallNames.contains(name)) {
                throw new InvalidParameterException(String.format("Duplicate recall name[%s]", name));
            }
            recallNames.add(name);
        }

        if (exposureClause != null) {
            exposureClause.validate();
        }
    }
}
