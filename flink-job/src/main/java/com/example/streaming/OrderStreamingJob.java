package com.example.streaming;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 实时链路入口：Kafka(order-events) -> Flink SQL 清洗 -> Doris(order_events)。
 *
 * <p>为什么用 Table API 而不是 DataStream + DorisSink：
 * 连接器已经把 Stream Load、重试、2PC、label 管理封装成 SQL connector，
 * 初学者只需要关注表结构和 SELECT，不需要手写 HTTP 客户端。
 *
 * <p>生产注意：
 * - Doris 表建议 Unique Key + 按天分区；示例沿用明细表；
 * - checkpoint 必须开启，否则 2PC 退化成至少一次；
 * - 作业并行度不要超过 Kafka 分区数，否则部分 subtask 空转。
 */
public final class OrderStreamingJob {

    private OrderStreamingJob() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnv();
        System.out.printf(
                "启动 Flink 作业: kafka=%s/%s, doris=%s/%s.%s, parallelism=%d, checkpoint=%dms%n",
                config.kafkaBootstrap(), config.kafkaTopic(),
                config.fenodes(), config.database(), config.table(),
                config.parallelism(), config.checkpointMs()
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 有界/无界统一按流处理；本地读文件调试时也不会因为批模式改变语义。
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(config.parallelism());
        env.enableCheckpointing(config.checkpointMs(), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 源表、解析视图、目标表、写入语句，四步顺序不能乱。
        tableEnv.executeSql(config.kafkaSourceDdl());
        tableEnv.executeSql(config.createViewSql());
        tableEnv.executeSql(config.dorisSinkDdl());
        tableEnv.executeSql(config.insertSql()).await();
    }
}
