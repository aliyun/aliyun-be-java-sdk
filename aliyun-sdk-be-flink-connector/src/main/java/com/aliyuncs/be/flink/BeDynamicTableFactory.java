package com.aliyuncs.be.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.configuration.ConfigOptions.key;

import java.util.HashSet;
import java.util.Set;

/**
 * @author silan.wpq
 * @date 2022/9/26
 */
public class BeDynamicTableFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "be";

    public static final ConfigOption<String> ENDPOINT = key("host")
            .stringType().noDefaultValue().withDescription("be host");
    public static final ConfigOption<Integer> PORT = key("port")
            .intType().defaultValue(80).withDescription("be port");
    public static final ConfigOption<String> USERNAME = key("username")
            .stringType().noDefaultValue().withDescription("be request username");
    public static final ConfigOption<String> PASSWORD = key("password")
            .stringType().noDefaultValue().withDescription("be request password");
    public static final ConfigOption<String> PK_FIELD = key("pk_field")
            .stringType().noDefaultValue().withDescription("be request primary key");
    public static final ConfigOption<String> CMD_FIELD = key("cmd_field")
            .stringType().noDefaultValue().withDescription("be request primary key");
    public static final ConfigOption<String> TABLE = key("be_table_name")
            .stringType().noDefaultValue().withDescription("be request table name");
    public static final ConfigOption<Integer> REQUEST_RETRY = key("request_retry")
            .intType().defaultValue(0).withDescription("be request table name");
    public static final ConfigOption<Boolean> DRY_RUN = key("dry_run")
            .booleanType().defaultValue(false).withDescription("dry_run");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        DataType dataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new BeDynamicSink(dataType, options);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ENDPOINT);
        options.add(PORT);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(PK_FIELD);
        options.add(CMD_FIELD);
        options.add(TABLE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REQUEST_RETRY);
        options.add(DRY_RUN);
        return options;
    }
}
