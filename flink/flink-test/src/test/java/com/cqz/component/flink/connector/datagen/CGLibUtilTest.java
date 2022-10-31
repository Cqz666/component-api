package com.cqz.component.flink.connector.datagen;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class CGLibUtilTest {

    @Test
    public void createBeanClass()  throws Exception{
        final Map<String, Class< ? >> properties = new HashMap<>();
        properties.put("foo", Integer.class);
        properties.put("bar", String.class);
        properties.put("baz", int[].class);

        final Class< ? > beanClass = CGLibUtil.createBeanClass("myClass", properties);
        Object obj = beanClass.newInstance();

        // set property"bar"
        beanClass.getMethod("setBar", String.class).invoke(obj,"Hello World!");

        // get property"bar"
        String result = (String) beanClass.getMethod("getBar").invoke(obj);
        System.out.println("Value for bar:" + result);

        for (Method method : beanClass.getMethods()) {
            System.out.println(method.getName());
        }

        for (Field field : beanClass.getDeclaredFields()) {
            System.out.println(field);
        }
    }
}