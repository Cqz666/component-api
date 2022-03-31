package com.cqz.component.flink.demo;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class StreamFileSinkDemo {
    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(10));
        env.setStateBackend(new FsStateBackend("file:///home/cqz/tmp/checkpoint"));

        //2.source
        DataStreamSource<String> lines = env.socketTextStream("127.0.0.1", 9999);

        //3.sink
        //设置sink的前缀和后缀
        //文件的头和文件扩展名
        //prefix-xxx-.txt
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".txt")
                .build();

        //设置sink的路径
        String outputPath = "hdfs://node1:8020/FlinkStreamFileSink/parquet";

        //创建StreamingFileSink
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(
                        new Path(outputPath),
                        new SimpleStringEncoder<String>("UTF-8"))
                /**
                 * 设置桶分配策略
                 * DateTimeBucketAssigner --默认的桶分配政策，默认基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH
                 * BasePathBucketAssigner ：将所有部分文件（part file）存储在基本路径中的分配器（单个全局桶）
                 */
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                /**
                 * 有三种滚动政策
                 *  CheckpointRollingPolicy
                 *  DefaultRollingPolicy
                 *  OnCheckpointRollingPolicy
                 */
                .withRollingPolicy(
                        /**
                         * 滚动策略决定了写出文件的状态变化过程
                         * 1. In-progress ：当前文件正在写入中
                         * 2. Pending ：当处于 In-progress 状态的文件关闭（closed）了，就变为 Pending 状态
                         * 3. Finished ：在成功的 Checkpoint 后，Pending 状态将变为 Finished 状态
                         *
                         * 观察到的现象
                         * 1.会根据本地时间和时区，先创建桶目录
                         * 2.文件名称规则：part-<subtaskIndex>-<partFileIndex>
                         * 3.在macos中默认不显示隐藏文件，需要显示隐藏文件才能看到处于In-progress和Pending状态的文件，因为文件是按照.开头命名的
                         *
                         */
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(2)) //设置滚动间隔
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(1)) //设置不活动时间间隔
                                .withMaxPartSize(1024 * 1024 * 1024) // 最大尺寸
                                .build())
                .withOutputFileConfig(config)
                .build();

        lines.addSink(sink).setParallelism(1);

        env.execute();
    }
}
