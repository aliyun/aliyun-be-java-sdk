package com.aliyuncs.be.flink;

import com.aliyuncs.be.client.BeClient;
import com.aliyuncs.be.client.BeWriteRequest;
import com.aliyuncs.be.client.protocol.BeWriteType;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.analysis.function.Max;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.*;
import java.util.stream.Collectors;

import static com.aliyuncs.be.flink.BeDynamicTableFactory.*;

/**
 * @author silan.wpq
 * @date 2022/9/26
 */
@Slf4j
public class BeTableSinkFunction extends AbstractBeRichSinkFunction<RowData> {

    private Set<String> fields;
    private DataStructureConverter converter;
    private DataType dataType;
    private ReadableConfig options;

    private String tableName;
    private String pkField;
    private String cmdField;
    private boolean dryRun;

    public BeTableSinkFunction(DataStructureConverter converter, DataType dataType, ReadableConfig options) {
        this.converter = converter;
        this.dataType = dataType;
        this.options = options;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RowType logicalType = (RowType) dataType.getLogicalType();
        fields = logicalType.getFields().stream()
                .map(RowType.RowField::getName)
                .collect(Collectors.toSet());
        tableName = options.get(TABLE);
        String endpoint = options.get(ENDPOINT);
        int port = options.get(PORT);
        String userName = options.get(USERNAME);
        String passWord = options.get(PASSWORD);
        client = new BeClient(endpoint, port, userName, passWord);
        pkField = options.get(PK_FIELD);
        cmdField = options.get(CMD_FIELD);
        retryNum = Math.max(0, options.get(REQUEST_RETRY));
        dryRun = options.get(DRY_RUN);
    }

    @Override
    public void invoke(RowData rowData, Context context) throws Exception {
        RowKind rowKind = rowData.getRowKind();
        if (!RowKind.INSERT.equals(rowKind)) {
            log.warn("Only INSERT RowKind supported, but:" + rowKind.name());
            return;
        }

        Row data = (Row) converter.toExternal(rowData);
        if (data == null) {
            return;
        }

        if (data.getField(pkField) == null) {
            log.warn("Pk field[{}] not found", pkField);
            return;
        }

        if (data.getField(cmdField) == null) {
            log.warn("Cmd field[{}] not found", cmdField);
            return;
        }

        String cmdValue = data.getField(cmdField).toString();
        if (!StringUtils.equalsIgnoreCase(BeWriteType.ADD.name(), cmdValue) &&
                !StringUtils.equalsIgnoreCase(BeWriteType.DELETE.name(), cmdValue)) {
            log.warn("Cmd value[{}] not supported, only support ADD and DELETE", cmdValue);
            return;
        }
        BeWriteType writeType = BeWriteType.valueOf(StringUtils.upperCase(cmdValue));

        Map<String, String> contents = new HashMap<>();
        for (String field : fields) {
            if (data.getField(field) != null) {
                contents.put(field, data.getField(field).toString());
            }
        }

        BeWriteRequest request = BeWriteRequest.builder()
                .primaryKey(pkField)
                .type(writeType)
                .contents(Lists.newArrayList(contents))
                .tableName(tableName)
                .build();

        if (dryRun) {
            log.info("Request:" + new Gson().toJson(request));
        } else {
            sendRequest(request);
        }
    }
}
