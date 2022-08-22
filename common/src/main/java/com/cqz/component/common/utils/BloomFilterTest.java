package com.cqz.component.common.utils;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.StandardCharsets;

public class BloomFilterTest {
    public static void main(String[] args) {
        // 1. 创建一个预估数据量200000条，错误率0.0001的布隆过滤器
        BloomFilter<CharSequence> bf=
                BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8),200000, 0.0001);
        // 2. 将10万条数据添加进去
        for (int i = 50000; i < 150000; i++) {
            bf.put("" + i);
        }
        System.out.println("数据写入完成");
        //3.测试结果
        for (int i = 0; i < 200000; i++) {
            if (bf.mightContain("" + i)) {
                System.out.println("您查找的数据："+i + "存在");
            } else {
                System.out.println("您查找的数据："+i + "不存在");
            }
        }

    }
}
