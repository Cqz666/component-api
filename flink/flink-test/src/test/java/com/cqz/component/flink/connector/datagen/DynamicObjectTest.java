package com.cqz.component.flink.connector.datagen;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DynamicObjectTest {

    private final Map propertyMap = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        propertyMap.put("name", String.class);
        propertyMap.put("age", Integer.class);
        propertyMap.put("height", Double.class);
    }

    @Test
    public void test() throws NoSuchFieldException, IllegalAccessException {
        DynamicObject bean = new DynamicObject(propertyMap);
        bean.put("name","mike");
        bean.put("age",20);
        bean.put("height",175.0);

        Map<String, Object> values = bean.getValues();
        for (String s : values.keySet()) {
            System.out.println(s);
            System.out.println(values.get(s));
        }
        Object name = bean.getToObject("name");
        Object age = bean.getToObject("age");
        Object height = bean.getToObject("height");
        System.out.println(name);
        System.out.println(age);
        System.out.println(height);

        System.out.println(StringUtils.capitalize("id"));
        System.out.println(StringUtils.capitalize("userId"));

    }


}