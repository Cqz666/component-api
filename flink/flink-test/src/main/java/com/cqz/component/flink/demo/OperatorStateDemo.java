package com.cqz.component.flink.demo;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class OperatorStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //不开启checkpoint情况下，如果重启会导致重复消费
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointTimeout(100);
//        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);
        env.setStateBackend(new FsStateBackend("file:///home/cqz/tmp/checkpoint"));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
        //固定延迟重启策略: 程序出现异常的时候，重启2次，每次延迟3秒钟重启，超过2次，程序退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));

        DataStreamSource<String> sourceData = env.addSource(new MyKafkaSource());
        sourceData.print();

        env.execute();
    }

    /**
     * MyKafkaSource就是模拟的FlinkKafkaConsumer并维护offset
     */
    public static class MyKafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
        //-1.声明一个OperatorState来记录offset
        private ListState<Long> offsetState = null;
        private Long offset = 0L;
        private boolean flag = true;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //-2.创建状态描述器
            ListStateDescriptor descriptor = new ListStateDescriptor("offsetState", Long.class);
            //-3.根据状态描述器初始化状态
            offsetState = context.getOperatorStateStore().getListState(descriptor);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            //-4.获取并使用State中的值
            Iterator<Long> iterator = offsetState.get().iterator();
            if (iterator.hasNext()){
                offset = iterator.next();
            }
            while (flag){
                offset += 1;
                int id = getRuntimeContext().getIndexOfThisSubtask();
//                System.out.println("分区:"+id+"消费到的offset位置为:" + offset);
                ctx.collect("分区:"+id+"消费到的offset位置为:" + offset);//1 2 3 4 5 6
                TimeUnit.SECONDS.sleep(1);
                if(offset % 5 == 0){
                    System.out.println("程序遇到异常了.....");
                    throw new Exception("程序遇到异常了.....");
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

        /**
         * 下面的snapshotState方法会按照固定的时间间隔将State信息存储到Checkpoint/磁盘中,也就是在磁盘做快照!
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //-5.保存State到Checkpoint中
            offsetState.clear();//清理内存中存储的offset到Checkpoint中
            //-6.将offset存入State中
            offsetState.add(offset);
            System.out.println("当前状态："+offset);
        }
    }
}
