package com.cqz.component.common.utils;

import java.util.Optional;
import java.util.Properties;

public class OptionalCase {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("a", "5");
        props.setProperty("b", "3");
        props.setProperty("c", "-3");
        int a = readDuration(props, "a");
        int _null = readDuration(props, "d");
        System.out.println(a+","+_null);

        int b = useOptional(props, "b");
        int null_ = useOptional(props, "d");
        System.out.println(b+","+null_);

    }

    public static int readDuration(Properties properties,String name){
        String value = properties.getProperty(name);
        if (value!=null){
            int i = Integer.parseInt(value);
            try {
                if (i>0){
                    return i;
                }
            } catch (NumberFormatException ignore) {
            }
        }
        return 0;
    }

    public static int useOptional(Properties properties,String name){
        return Optional.ofNullable(properties.getProperty(name))
                .flatMap(OptionalCase::stringToInt)
                .filter(i->i>0)
                .orElse(0);
    }

    private static Optional<Integer> stringToInt(String s) {
        try {
            return Optional.of(Integer.parseInt(s));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

}
