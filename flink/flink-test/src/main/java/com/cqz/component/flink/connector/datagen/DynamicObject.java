package com.cqz.component.flink.connector.datagen;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.cglib.beans.BeanGenerator;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DynamicObject {

    private Object dynamicBean;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    Class clazz;

    public DynamicObject(){
    }

    public DynamicObject(Map dynAttrMap) {
        this.dynamicBean = generateBean(dynAttrMap);
        this.clazz = dynamicBean.getClass();
    }

    public DynamicObject(Object object) {
        dynamicBean = generateBean(getFields(object));
        Map<String, Object> values = getValues(object);
        Iterator<String> iterator = values.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            Object value = values.get(key);
            try {
                put(key, value);
            } catch (IllegalAccessException | NoSuchFieldException e) {
                e.printStackTrace();
            }
        }
        clazz = dynamicBean.getClass();
    }

    public static DynamicObject parseMap(Map<String, Object> targetMap) {
        DynamicObject dynamicObject = new DynamicObject();
        for (Map.Entry<String, Object> entry : targetMap.entrySet()) {
            try {
                dynamicObject.put(entry.getKey(), entry.getValue());
            } catch (IllegalAccessException | NoSuchFieldException e) {
                e.printStackTrace();
            }
        }
        return dynamicObject;
    }

    public static DynamicObject parseString(String jsonString) {
        JSONObject jsonObject = JSONObject.parseObject(jsonString);
        return parseMap(jsonObject);
    }

    /**
     * 获取所有属性值
     *
     * @return
     * @throws IllegalAccessException
     */
    public Map<String, Object> getValues() throws IllegalAccessException {
        Map<String, Object> fieldValuesMap = new HashMap<>(16);
        if (clazz != null) {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object fieldValue = field.get(dynamicBean);
                fieldValuesMap.put(field.getName().split("\\$cglib_prop_")[1], fieldValue);
            }
            return fieldValuesMap;
        }
        return fieldValuesMap;
    }

    /**
     * 获取所有属性值
     *
     * @return
     * @throws IllegalAccessException
     */
    public Map<String, Object> getValues(Object object) {
        Map<String, Object> fieldValuesMap = new HashMap<>(16);
        Class<?> clazz = object.getClass();
        if (clazz != null) {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object fieldValue = null;
                try {
                    fieldValue = field.get(object);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                fieldValuesMap.put(field.getName(), fieldValue);
            }
            return fieldValuesMap;
        }
        return fieldValuesMap;
    }



    /**
     * 设置属性值，不存在就添加
     *
     * @param property
     * @param value
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    public void put(String property, Object value) throws IllegalAccessException, NoSuchFieldException {
        Field declaredField;
        try {
            declaredField = clazz.getDeclaredField("$cglib_prop_" + property);
        } catch (Exception e) {
            Map<String, Class<?>> fields = getFields();
            fields.put(property, Object.class);

            Map<String, Object> values = getValues();

            this.dynamicBean = generateBean(fields);
            this.clazz = dynamicBean.getClass();

            values.put(property, value);
            Iterator<String> iterator = values.keySet().iterator();
            while (iterator.hasNext()) {
                String putKey = iterator.next();
                Object putValue = values.get(putKey);
                Field field = clazz.getDeclaredField("$cglib_prop_" + putKey);
                field.setAccessible(true);
                field.set(dynamicBean, putValue);
            }
            return;
        }
        declaredField.setAccessible(true);
        declaredField.set(dynamicBean, value);
    }

    /**
     * 在已有的实体上添加属性
     *
     * @param object
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    public void putAll(Object object) throws IllegalAccessException, NoSuchFieldException {
        Class<?> clazz = object.getClass();
        Field[] declaredFields = clazz.getDeclaredFields();
        Map<String, Object> fieldValuesMap = new HashMap<>(16);
        Map<String, Object> fieldTypeMap = new HashMap<>(16);
        for (Field field : declaredFields) {
            field.setAccessible(true);
            Object fieldValue = field.get(object);
            fieldValuesMap.put(field.getName(), fieldValue);
            fieldTypeMap.put(field.getName(), field.getType());
        }
        //获取当前的属性及属性值
        fieldTypeMap.putAll(getFields());
        fieldValuesMap.putAll(getValues());
        this.dynamicBean = generateBean(fieldTypeMap);
        this.clazz = dynamicBean.getClass();
        Iterator<String> iterator = fieldValuesMap.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            Object value = fieldValuesMap.get(key);
            put(key, value);
        }
    }

    public Map<String, Class<?>> getFields() throws IllegalAccessException {
        Map<String, Class<?>> attrMap = new HashMap<>(16);
        if (clazz != null) {
            Iterator<String> iterator = getValues().keySet().iterator();

            while (iterator.hasNext()) {
                attrMap.put(iterator.next(), Object.class);
            }
        }
        return attrMap;
    }

    /**
     * 获取对象所有属性及对应的类别
     *
     * @param object
     * @return
     * @throws IllegalAccessException
     */
    public Map<String, Class<?>> getFields(Object object) {
        Class<?> clazz = object.getClass();
        Map<String, Class<?>> attrMap = new HashMap<>(16);
        if (clazz != null) {
            Iterator<String> iterator = null;
            iterator = getValues(object).keySet().iterator();

            while (iterator.hasNext()) {
                attrMap.put(iterator.next(), Object.class);
            }
        }
        return attrMap;
    }

    /**
     * 获取属性值
     *
     * @param property 设置的字段
     * @return JsonNode对象
     */
    public JsonNode get(String property) {
        JsonNode jsonNode = objectMapper.valueToTree(dynamicBean);
        return jsonNode.get(property);
    }

    /**
     * 获取属性值
     *
     * @param property 设置的字段
     * @return 属性对应的对象
     * @throws NoSuchFieldException   没有字段
     * @throws IllegalAccessException 反射错误
     */
    public <E> E getToObject(String property) {
        Field declaredField = null;
        try {
            declaredField = clazz.getDeclaredField("$cglib_prop_" + property);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        declaredField.setAccessible(true);
        try {
            return (E) declaredField.get(dynamicBean);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Object getEntity() {
        return this.dynamicBean;
    }

    public Class<?> getClazz(){
        return this.clazz;
    }

    private Object generateBean(Map dynAttrMap) {
        BeanGenerator generator = new BeanGenerator();
        if (dynAttrMap != null) {
            for (Object o : dynAttrMap.keySet()) {
                String key = o.toString();
                generator.addProperty(key, (Class) dynAttrMap.get(key));
            }
        }
        return generator.create();
    }




}
