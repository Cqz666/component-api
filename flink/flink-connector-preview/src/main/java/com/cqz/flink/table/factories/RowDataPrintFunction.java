package com.cqz.flink.table.factories;

import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class RowDataPrintFunction extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;
    private DynamicTableSink.DataStructureConverter converter;
    private  PrintSinkOutputWriter<String> writer;
    private final Long numberOfRows;
    private final Long numberOfOutputsPerSecond;
    private transient int toOutput;
    private transient int outputSoFar;
    private transient long markTime = System.currentTimeMillis();

    public RowDataPrintFunction(DynamicTableSink.DataStructureConverter converter, Long numberOfRows,Long numberOfOutputsPerSecond) {
        this.converter = converter;
        this.writer = new PrintSinkOutputWriter<>();
        this.numberOfRows = numberOfRows;
        this.numberOfOutputsPerSecond =numberOfOutputsPerSecond;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
        writer.open(context.getIndexOfThisSubtask(),context.getNumberOfParallelSubtasks());

        if (numberOfOutputsPerSecond != null){
            final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
            final int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            final int baseSizeOfPerSecond = (int) (numberOfOutputsPerSecond / stepSize);
            toOutput = (numberOfOutputsPerSecond % stepSize > taskIdx) ? baseSizeOfPerSecond + 1 : baseSizeOfPerSecond;
        }

    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {

        if (numberOfOutputsPerSecond == null || ( outputSoFar<toOutput) ){
            Object data = converter.toExternal(value);
            assert data != null;
                writer.write(data.toString());
                outputSoFar++;
            }
        else {
            if (System.currentTimeMillis() - markTime>1000){
                writer.write("===========");
                outputSoFar=0;
                markTime=System.currentTimeMillis();
            }

        }

        }

    }

