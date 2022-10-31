package com.cqz.component.flink.connector.datagen.pojo;

import com.cqz.component.flink.connector.datagen.DataGenDescriptor;
import org.apache.flink.table.factories.FactoryUtil;

import java.lang.reflect.Field;

public class DataGenOptionTest {
    public static void main(String[] args) {
        for (Field field : UserInfo.class.getDeclaredFields()) {
            System.out.println("name="+field.getName());
            System.out.println("type="+field.getType().getSimpleName());
            DataGenOption annotation = field.getAnnotation(DataGenOption.class);
            if (annotation!=null){
                System.out.println("kind="+annotation.kind()+",min="+annotation.min()+",max="+annotation.max()+",start="+annotation.start());
            }
        }
        System.out.println(FactoryUtil.CONNECTOR.key());
//
        DataGenDescriptor dataGenerator = DataGenDescriptor.forPojo(UserInfo.class)
                .rowsPerSecond(1000)
                .numberOfRows(1000)
                .build();

        System.out.println(dataGenerator.getNumberOfRows());
        System.out.println(dataGenerator.getRowsPerSecond());
        System.out.println(dataGenerator.getRowsPerSecond().toString());



    }
}
