package com.aliyuncs.be.client;

import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author silan.wpq
 * @date 2021/7/15
 */
@Data
public class BeResult {
    private int errorCode;
    private String errorMessage;
    private MatchItems matchItems;

    @Data
    public static class MatchItems {
        List<String> fieldNames;
        List<List<Object>> fieldValues;
    }

    public int getItemsCount() {
        if (matchItems == null || matchItems.getFieldValues() == null) {
            return 0;
        }
        return matchItems.getFieldValues().size();
    }

    public Map<String, Object> getItem(int num) {
        if (num < 0 || num >= getItemsCount()) {
            return null;
        }
        List<Object> value = matchItems.getFieldValues().get(num);
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < matchItems.fieldNames.size(); i++) {
            result.put(matchItems.fieldNames.get(i), value.get(i));
        }
        return result;
    }
}
