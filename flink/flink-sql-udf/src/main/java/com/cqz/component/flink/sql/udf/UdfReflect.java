package com.cqz.component.flink.sql.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.URL;
import java.net.URLClassLoader;

public class UdfReflect {
    public static void main(String[] args) throws Exception {
        File file =new File("/home/cqz/IdeaProjects/component-api/flink/flink-sql-udf/target/flink-sql-udf-1.0-SNAPSHOT.jar");
        URLClassLoader jarClassLoader  =
                new URLClassLoader(new URL[]{file.toURI().toURL()},Thread.currentThread().getContextClassLoader());

        Class<?> myClass = jarClassLoader.loadClass("com.cqz.component.flink.sql.udf.SplitFunction");
        for (Method method : myClass.getMethods()) {
            if (method.getName().equals("eval")) {
                System.out.println("eval return =>"+method.getGenericReturnType());
                for (Class<?> parameterType : method.getParameterTypes()) {
                    System.out.println("eval para =>"+parameterType.getTypeName());
                }
            }
        }
        FunctionHint annotation1 = myClass.getAnnotation(FunctionHint.class);
        DataTypeHint output = annotation1.output();
        String value = output.value();
        System.out.println(value);

        System.out.println("===================");
        Class<?> superclass = myClass.getSuperclass();
        for (Method declaredMethod : superclass.getDeclaredMethods()) {
            if ("collect".equals(declaredMethod.getName())){
                for (Parameter parameter : declaredMethod.getParameters()) {
                    System.out.println(parameter.getType().getCanonicalName());
                }
                System.out.println(declaredMethod.toGenericString());
                System.out.println(declaredMethod.toString());
            }
        }

    }
}
