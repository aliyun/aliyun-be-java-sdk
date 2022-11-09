package com.aliyuncs.be.flink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;

/**
 * @author silan.wpq
 * @date 2022/9/26
 */
public class BeDynamicSink implements DynamicTableSink {

    private ReadableConfig options;
    private DataType dataType;

    public BeDynamicSink(DataType dataType, ReadableConfig options) {
        this.dataType = dataType;
        this.options = options;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter = context.createDataStructureConverter(dataType);
        return SinkFunctionProvider.of(new BeTableSinkFunction(converter, dataType, options));
    }

    @Override
    public DynamicTableSink copy() {
        return new BeDynamicSink(dataType, options);
    }

    @Override
    public String asSummaryString() {
        return "be";
    }
}
