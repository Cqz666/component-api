package com.cqz.component.common.protobuf;

import com.cqz.protobuf.ConfigProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class ProtobufTest {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        // 创建DogConfig 对象
        ConfigProto.DogConfig dogConfig= ConfigProto.DogConfig.newBuilder()
                .setName("huahua")
                .setAge(3)
                .setAdopted(true)
                .build();
        System.out.println("dogConfig = " + dogConfig.toString());
        //序列化
        ByteString bytes = dogConfig.toByteString();
        //反序列化
        ConfigProto.DogConfig dogConfig1 = ConfigProto.DogConfig.parseFrom(bytes);
        System.out.println("dogConfig = " + dogConfig1.toString());
    }
}
