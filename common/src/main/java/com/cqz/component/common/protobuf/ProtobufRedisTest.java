package com.cqz.component.common.protobuf;

import com.cqz.protobuf.ConfigProto;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.xm4399.gprp.util.redis.JedisClusterPipeline;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.xerial.snappy.Snappy;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

public class ProtobufRedisTest {
    public static final String REDIS_CLUSTER = "10.21.0.117:6390,10.21.0.118:6395,10.21.0.119:6396";

    public static void main(String[] args) throws Exception {
        JedisClusterPipeline jedisCluster = new JedisClusterPipeline(
                getClusterNode(REDIS_CLUSTER),
                1000,
                1000,
                3,
                getJedisConfig());
        // 创建protobuf 对象
        ConfigProto.DogConfig petDog= ConfigProto.DogConfig.newBuilder()
                .setName("kk")
                .setAge(3)
                .setAdopted(true)
                .build();
//        ByteString bytes = petDog.toByteString();
        byte[] compressedVal = Snappy.compress(petDog.toByteArray());
        //压缩
        byte[] key = "dog-kk".getBytes(StandardCharsets.UTF_8);
        jedisCluster.set(key, compressedVal);

        String value = Snappy.uncompressString( jedisCluster.get(key));
        ConfigProto.DogConfig dogConfig = ConfigProto.DogConfig.parseFrom(ByteString.copyFromUtf8(value));

        System.out.println(dogConfig);

        String key1 = "1094gGENVvinoFOAAEfJI6ec4@_f3@_count@_*@_v1";
        String field = "20230109";
        byte[] hget = jedisCluster.hget(key1.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8));
        String value1 = Snappy.uncompressString(hget);
//        ValueInfo.Msg msg = ValueInfo.Msg.parseFrom(ByteString.copyFromUtf8(value1));
//        for (String s : msg.getDataList()) {
//            System.out.println(s);
//        }
        System.out.println(value1);
//        jedisCluster.rpush("k1","v1");
//        jedisCluster.rpush("k1","v2");
//        jedisCluster.rpush("k1","v3");
//
//        List<String> k1 = jedisCluster.lrange("k1", 0, -1);
//        k1.forEach(System.out::println);
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

    private static JedisPoolConfig getJedisConfig() {
        JedisPoolConfig config = new JedisPoolConfig();
        //pool 最大线程数
        config.setMaxTotal(50);
        // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        config.setMaxIdle(30);
        config.setMinIdle(30);
        //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
        config.setMaxWaitMillis(2000);
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
