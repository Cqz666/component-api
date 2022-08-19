package com.cqz.component.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.descriptors.*;

import java.util.Arrays;

public class TableApiTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv  = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
//        final Schema schema = new Schema()
//                .field("a", DataTypes.INT())
//                .field("b", DataTypes.STRING())
//                .field("c", DataTypes.BIGINT());
//
//        tableEnv.connect(new FileSystem().path("/path/to/file"))
//                .withFormat(new Csv().fieldDelimiter('|').deriveSchema())
//                .withSchema(schema)
//                .createTemporaryTable("CsvSinkTable");

        ResolvedSchema resolvedSchema  = ResolvedSchema.of(Arrays.asList(
                Column.physical("a", DataTypes.INT()),
                Column.physical("b", DataTypes.STRING()),
                Column.physical("c", DataTypes.BIGINT())
        ));

        TableSchema tableSchema = TableSchema.fromResolvedSchema(resolvedSchema);

        Schema schema = new Schema().schema(tableSchema);

        tableEnv.connect(new Kafka().topic("").property("",""))
                .withFormat(new Json().failOnMissingField(false).ignoreParseErrors(true))
                .withSchema(schema)
                .createTemporaryTable("kafkaTable");






    }
}
