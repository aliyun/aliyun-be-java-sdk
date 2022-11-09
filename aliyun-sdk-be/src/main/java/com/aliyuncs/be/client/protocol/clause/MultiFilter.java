package com.aliyuncs.be.client.protocol.clause;

import com.aliyuncs.be.client.exception.InvalidParameterException;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author silan.wpq
 * @date 2021/8/2
 */
@Data
public class MultiFilter implements BeFilter {
    List<BeFilter> filters = new ArrayList<>();
    FilterConnector filterConnector = FilterConnector.AND;

    public MultiFilter(FilterConnector filterConnector, List<BeFilter> filters) {
        this.filters = filters;
        this.filterConnector = filterConnector;
    }

    public MultiFilter(FilterConnector filterConnector, BeFilter... filters) {
        this.filters = Arrays.asList(filters);
        this.filterConnector = filterConnector;
    }

    public MultiFilter addFilter(BeFilter filter) {
        filters.add(filter);
        return this;
    }

    public MultiFilter addFilter(String left, FilterOperator operator, String right) {
        SingleFilter filter = new SingleFilter(left, operator, right);
        filters.add(filter);
        return this;
    }

    @Override
    public String getConditionValue() throws InvalidParameterException {
        if (CollectionUtils.isEmpty(filters)) {
            return StringUtils.EMPTY;
        }
        List<String> conditionClauses = new ArrayList<>();
        for(BeFilter condition : filters) {
            if (condition == null) {
                continue;
            }
            if (condition instanceof MultiFilter) {
                conditionClauses.add("(" + condition.getConditionValue() + ")");
            } else {
                conditionClauses.add(condition.getConditionValue());
            }
        }
        if (CollectionUtils.isEmpty(conditionClauses)) {
            return StringUtils.EMPTY;
        }
        if (conditionClauses.size() == 1) {
            return conditionClauses.get(0);
        }

        if (filterConnector == null) {
            throw new InvalidParameterException("Connector should not be null for multi filter conditions");
        }
        return StringUtils.join(conditionClauses, String.format(" %s ", filterConnector.name()));
    }

    @Override
    public InFilter getInFilter() {
        for (BeFilter filter : filters) {
            if (filter != null && filter.getInFilter() != null) {
                return filter.getInFilter();
            }
        }
        return null;
    }
}
