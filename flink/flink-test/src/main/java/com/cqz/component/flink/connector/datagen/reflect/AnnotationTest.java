package com.cqz.component.flink.connector.datagen.reflect;

import com.cqz.component.flink.connector.datagen.pojo.DataGenOption;
import com.cqz.component.flink.connector.datagen.pojo.UserInfo;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Map;

public class AnnotationTest {
    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        UserInfo userInfo = new UserInfo();
        Field field = UserInfo.class.getDeclaredField("id");
        DataGenOption option = field.getAnnotation(DataGenOption.class);
        //修改前的注解属性值
        System.out.println(option.kind());
        System.out.println(option.min());
        System.out.println(option.max());
        System.out.println(option.length());
        System.out.println(option.start());
        System.out.println(option.end());
        //获取 foo 这个代理实例所持有的 InvocationHandler
        InvocationHandler h = Proxy.getInvocationHandler(option);
        // 获取 AnnotationInvocationHandler 的 memberValues 字段
        Field hField = h.getClass().getDeclaredField("memberValues");
        hField.setAccessible(true);
        // 获取 memberValues
        Map memberValues = (Map) hField.get(h);
        // 修改 value 属性值
        memberValues.put("kind", "asasa");
        memberValues.put("min", 1111L);
        memberValues.put("max", 2222222L);
        memberValues.put("length", 123411);
        memberValues.put("start", 11L);
        memberValues.put("end", 33333L);
        // 修改后的注解属性值
        System.out.println("------------------调整后-------------------");
        System.out.println(option.kind());
        System.out.println(option.min());
        System.out.println(option.max());
        System.out.println(option.length());
        System.out.println(option.start());
        System.out.println(option.end());
    }
}
