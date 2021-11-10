package com.cqz.flink.table.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

public class PreviewTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "my_preview";

    public static final ConfigOption<Long> MAX_NUMBER_OF_OUTPUTS_PER_SECOND =
            key("max-number-of-outputs-per-second")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Max number of rows to print in one second.");

    public static final ConfigOption<String> PRINT_IDENTIFIER =
            key("print-identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Message that identify print and is prefixed to the output of the value.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        return new PreviewSink(
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType(),
                options.get(PRINT_IDENTIFIER),
                options.get(MAX_NUMBER_OF_OUTPUTS_PER_SECOND),
                options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null)
                );
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PRINT_IDENTIFIER);
        options.add(MAX_NUMBER_OF_OUTPUTS_PER_SECOND);
        options.add(FactoryUtil.SINK_PARALLELISM);
        return options;
    }

}
