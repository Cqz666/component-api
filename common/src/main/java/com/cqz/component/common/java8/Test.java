package com.cqz.component.common.java8;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        MyFuntion[] myFuntions = new MyFuntion[3];

        for (int i = 0; i < 3; i++) {
            myFuntions[i] = createToConvert(i);
        }
        for (int i = 0; i < 3; i++) {
            Object convert = myFuntions[i].convert("code");
            System.out.println(convert.toString());
        }

        int i = Arrays.asList("a", "b", "c").indexOf("a");
        System.out.println(i);

    }

    private static MyFuntion createToConvert(int i){
        switch (i){
            case 0:
                return (s)->"0-"+s;
            case 1:
                return (s)->"1-"+s;
            case 2:
                return (s)->"2-"+s;
            default:
                throw new UnsupportedOperationException("Unsupported" );
        }
    }
}
