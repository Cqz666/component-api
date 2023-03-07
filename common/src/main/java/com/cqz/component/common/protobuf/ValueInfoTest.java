package com.cqz.component.common.protobuf;

import com.cqz.protobuf.ValueInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class ValueInfoTest {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        ValueInfo.Msg msg = ValueInfo.Msg.newBuilder()
                .addData("aa")
                .addData("bb")
                .build();
        System.out.println(msg);
        String data = msg.getData(1);
        System.out.println(data);

        List<String> subList = ValueInfo.Msg.newBuilder()
                .addData("aa")
                .addData("bb")
                .addData("cc")
                .getDataList();
        for (String s : subList) {
            System.out.println(s);
        }
        System.out.println("--------");

        List<String> list = new ArrayList<>(subList);
        Iterator<String> iterator = list.iterator();
        if (iterator.hasNext()){
            String next = iterator.next();
            if ("aa".equals(next)){
                iterator.remove();
            }
        }
        ValueInfo.Msg build = ValueInfo.Msg.newBuilder().addAllData(list).build();
        System.out.println(build);

        ValueInfo.Msg msg1 = ValueInfo.Msg.parseFrom(ByteString.EMPTY);
        List<String> subList2  = msg1.getDataList();


    }
}
