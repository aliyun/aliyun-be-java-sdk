package com.aliyuncs.be.client.protocol;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import com.aliyuncs.be.client.protocol.clause.score.ScorerClause;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;

@Builder
@Data
public class RecallParam {
    private String recallName;
    private Collection<String> triggerItems;
    private RecallType recallType;
    private ScorerClause scorerClause;
    private int returnCount = 10;

    public void validate() throws InvalidParameterException {
        if (CollectionUtils.isEmpty(triggerItems)) {
            throw new InvalidParameterException(String.format("Empty trigger items for recall[%s]", recallName));
        }
        if (recallType == null) {
            throw new InvalidParameterException("Recall type not set");
        }
        if (returnCount < 0) {
            throw new InvalidParameterException(String.format("Return count should be greater than 0 for recall[%s]", recallName));
        }
    }

    public String getTriggerKey() {
        return StringUtils.isNotBlank(recallName) ? recallName + "_trigger_list" : "trigger_list";
    }

    public String getScorerKey() {
        return StringUtils.isNotBlank(recallName) ? recallName + "_score_rule" : "score_rule";
    }

    public String getReturnCountKey() {
        return recallName + "_return_count";
    }

    public String flatTriggerItems() {
        if (RecallType.X2I.equals(recallType)) {
            return StringUtils.join(triggerItems, ",");
        } else {
            return StringUtils.join(triggerItems, ";");
        }
    }
}
