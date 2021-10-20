package com.cqz.flink.table.factories;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

public class PreviewSink implements DynamicTableSink {

    private final DataType type;
    private final Integer parallelism;
    private final Long numberOfRows;
    private final Long numberOfOutputsPerSecond;

    public PreviewSink(
            DataType type,
            Integer parallelism,
            Long numberOfRows,
            Long numberOfOutputsPerSecond
            ) {
        this.type = type;
        this.parallelism = parallelism;
        this.numberOfRows = numberOfRows;
        this.numberOfOutputsPerSecond = numberOfOutputsPerSecond;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
//        return ChangelogMode.upsert();
//        return requestedMode;
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
//                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter = context.createDataStructureConverter(type);
        return SinkFunctionProvider.of(
                new RowDataPrintFunction(converter,numberOfRows,numberOfOutputsPerSecond),parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new PreviewSink(type, parallelism,numberOfRows,numberOfOutputsPerSecond);
    }

    @Override
    public String asSummaryString() {
        return "Print to System.out";
    }
}
