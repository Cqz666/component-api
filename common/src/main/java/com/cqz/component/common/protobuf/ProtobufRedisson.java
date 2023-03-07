package com.cqz.component.common.protobuf;

import com.cqz.protobuf.ConfigProto;
import com.google.protobuf.ByteString;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.config.Config;
import org.xerial.snappy.Snappy;

import java.io.IOException;

public class ProtobufRedisson {
    public static void main(String[] args) throws IOException {
        RedissonClient redissonClient = getRedissonClient();
        RBucket<byte[]> bucket =
                redissonClient.getBucket("dog-kk", ByteArrayCodec.INSTANCE);
        byte[] bytes = bucket.get();
        String value = Snappy.uncompressString(bytes);
        ConfigProto.DogConfig dogConfig = ConfigProto.DogConfig.parseFrom(ByteString.copyFromUtf8(value));

        System.out.println(dogConfig);
    }
    private static RedissonClient getRedissonClient(){
        Config config = new Config();
        config.useClusterServers()
                .setScanInterval(2000) // cluster state scan interval in milliseconds
                .addNodeAddress("redis://10.21.0.117:6390")
                .addNodeAddress("redis://10.21.0.118:6395")
                .addNodeAddress("redis://10.21.0.119:6396");
        return Redisson.create(config);
    }
}
