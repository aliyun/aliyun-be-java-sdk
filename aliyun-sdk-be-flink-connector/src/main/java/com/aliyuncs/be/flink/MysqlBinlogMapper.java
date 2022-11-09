package com.aliyuncs.be.flink;

import com.aliyuncs.be.client.BeWriteRequest;
import com.aliyuncs.be.client.protocol.BeWriteType;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author silan.wpq
 * @date 2022/5/7
 */
@Slf4j
public class MysqlBinlogMapper extends RichMapFunction<String, BeWriteRequest> {
    private String beTableName;
    private String dbTablePK;
    private String beTablePk;
    private Map<String, String> keyNameConvertMap;

    public MysqlBinlogMapper(String beTableName, String dbTablePK, Map<String, String> keyNameConvertMap) {
        this.beTableName = beTableName;
        this.dbTablePK = dbTablePK;
        this.keyNameConvertMap = keyNameConvertMap == null ? new HashMap<>() : keyNameConvertMap;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        beTablePk = keyNameConvertMap.getOrDefault(dbTablePK, dbTablePK);
    }

    @Override
    public BeWriteRequest map(String s) throws Exception {
        JsonObject rawRecord = new Gson().fromJson(s, JsonObject.class);
        String op = rawRecord.get("op").getAsString();
        BeWriteRequest request = null;
        if ("u".equals(op) || "c".equals(op)) {
            JsonObject recordAdd = rawRecord.get("after").getAsJsonObject();
            Map<String, String> content = new HashMap<>();
            recordAdd.entrySet().forEach( e ->
                    content.put(keyNameConvertMap.getOrDefault(e.getKey(), e.getKey()), e.getValue().getAsString()));
            request = BeWriteRequest.builder()
                    .primaryKey(beTablePk)
                    .tableName(beTableName)
                    .contents(Collections.singletonList(content))
                    .type(BeWriteType.ADD)
                    .build();
        } else if ("d".equals(op)) {
            JsonObject recordDelete = rawRecord.get("before").getAsJsonObject();
            Map<String, String> content = new HashMap<>();
            content.put(beTablePk, recordDelete.get(dbTablePK).getAsString());
            request = BeWriteRequest.builder()
                    .primaryKey(beTablePk)
                    .tableName(beTableName)
                    .contents(Collections.singletonList(content))
                    .type(BeWriteType.DELETE)
                    .build();
        } else {
            log.debug("op[{}] not support for binlog[{}]", op, s);
        }
        return request;
    }
}
