import com.cqz.flink.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import static com.cqz.flink.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;


public class SQLInsertTest {

    public static final String CLUSTERNODES = "10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000,10.11.80.147:8001,10.11.80.147:9000,10.11.80.147:9001";

    @Test
    public void testNoPrimaryKeyInsertSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis(id INT,name VARCHAR, age INT, country VARCHAR) with ( 'connector'='redis', " +
                "'host'='127.0.0.1'," +
                "'port'='6380', " +
                "'redis-mode'='single'," +
                "'xpush-key'='job_id:sink_table'," +
                "'key-ttl'='60','" +
//                "'key-column'='id','" +
//                "'value-column'='name','" +
                REDIS_COMMAND + "'='" + RedisCommand.XPUSH + "')" ;

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values (2,'JackMa',50, 'CHINA'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);

        Jedis jedis = new Jedis("127.0.0.1", 6380);
        String rpop = jedis.rpop("job_id:sink_table_name");
        System.out.println(rpop);
    }


    @Test
    public void testSingleInsertHashClusterSQL() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis(username VARCHAR, level varchar, age varchar) with ( 'connector'='redis', " +
                "'cluster-nodes'='" + CLUSTERNODES + "','redis-mode'='cluster','field-column'='level', 'key-column'='username', 'put-if-absent'='true'," +
                " 'value-column'='age', 'password'='******','" +
                REDIS_COMMAND + "'='" + RedisCommand.HSET + "', 'maxIdle'='2', 'minIdle'='1'  )" ;

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test_hash', '3', '15'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
        System.out.println(sql);
    }


}