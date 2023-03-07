package com.cqz.component.common.java8;

import com.google.common.collect.Maps;

import java.util.Map;

public class MapTest {
    public static void main(String[] args) {
        Map<Integer, String> map = Maps.newConcurrentMap();
        map.computeIfAbsent(1, k->"s");
        map.computeIfAbsent(1, k->"b");
        System.out.println(map.toString());
    }
}
