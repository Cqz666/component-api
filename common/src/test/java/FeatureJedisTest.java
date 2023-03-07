import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.xm4399.gprp.util.redis.JedisClusterPipeline;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.xerial.snappy.Snappy;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FeatureJedisTest {
//    public static final String REDIS_CLUSTER = "10.21.0.117:6390,10.21.0.118:6395,10.21.0.119:6396";
//    public static final String REDIS_CLUSTER = "10.21.0.106:6388,10.21.0.107:6391,10.21.0.108:6385,10.21.0.111:6389,10.21.0.110:6386,10.21.0.109:6385";
    public static final String REDIS_CLUSTER = "10.21.0.111:6381,10.21.0.112:6381,10.21.0.113:6381,10.21.0.114:6383,10.21.0.115:6381";
    private static final int MAX_ACTIVE = 50;
    private static final int MAX_IDLE = 30;
    private static final int MIN_IDLE = 30;
    private static final long MAX_WAIT = 2000;

    public static void main(String[] args) throws Exception {
        JedisClusterPipeline jedisClusterPipeline = new JedisClusterPipeline(getClusterNode(REDIS_CLUSTER), 1000, 1000, 3, getJedisConfig());
            String key = "46:2679028038";

//        String key = "6:1077gIw4UMWCR5Kpj4KC06c9b";
        if (args.length>=1){
            key = args[0];
        }
        byte[] byteKey
//                = key.getBytes(StandardCharsets.UTF_8);
                = Snappy.compress(key.getBytes(StandardCharsets.UTF_8));

        byte[] bytes = jedisClusterPipeline.get(byteKey);
        Long ttl = jedisClusterPipeline.ttl(byteKey);
//        Long del = jedisClusterPipeline.del(byteKey);
//        System.out.println("del"+del);
        System.out.println(ttl);
        if (bytes!=null){
            String s = Snappy.uncompressString(bytes);
            System.out.println(s);
            System.out.println("特征数："+s.split("@&").length);
        }else {
            System.out.println("当前redis key is null ");
        }

        List<byte[]> allKeys = new ArrayList<>();
        allKeys.add(byteKey);
        Map<byte[], Response<byte[]>> responseMap = jedisClusterPipeline.pipelineGetByte(allKeys);
        byte[] bytes1 = responseMap.get(byteKey).get();
        if (bytes1!=null){
            System.out.println("pipe"+bytes1.length);
            String s1 = Snappy.uncompressString(bytes1);
            System.out.println(s1);
        }else {
            System.out.println("当前redis key is null ");
        }

        jedisClusterPipeline.close();
    }

    private static Set<HostAndPort> getClusterNode(String clusterAddr) throws Exception {

        if (StringUtils.isEmpty(clusterAddr)) throw new Exception("集群地址异常！请先检查");
        List<String> addrs = Splitter.on(",").omitEmptyStrings().splitToList(clusterAddr);
        Set<HostAndPort> clusterNodes = Sets.newHashSet();
        for (String node : addrs) {
            if (!node.contains(":")){
                throw new Exception("地址格式异常！请先检查");
            }
            List<String> hostAndPost = Splitter.on(":").splitToList(node);
            clusterNodes.add(new HostAndPort(hostAndPost.get(0), NumberUtils.toInt(hostAndPost.get(1))));
        }
        if (clusterNodes.size() == 0){
            throw new Exception("没有捕捉到任何节点，请先检查");
        }
        return clusterNodes;
    }

    /**
     * 构造集群配置
     */
    private static JedisPoolConfig getJedisConfig() {
        JedisPoolConfig config = new JedisPoolConfig();
        //pool 最大线程数
        config.setMaxTotal(MAX_ACTIVE);
        // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        config.setMaxIdle(MAX_IDLE);
        config.setMinIdle(MIN_IDLE);
        //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
        config.setMaxWaitMillis(MAX_WAIT);
        // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
        config.setTestOnBorrow(false);
        config.setTestOnReturn(false);
        config.setTestWhileIdle(true);
        config.setTimeBetweenEvictionRunsMillis(60000);
        config.setTimeBetweenEvictionRunsMillis(30000);
        config.setNumTestsPerEvictionRun(-1);
        return config;
    }

}
