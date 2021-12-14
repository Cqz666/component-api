package com.cqz.component.common.async;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class CompletableFutureDemo {
    /**
     * json样式的 字符串
     */
    static class ToStrJson {
        public static String toJson(IsDigits isDigits) {
            delay();
            final String result = ToStringBuilder.reflectionToString(isDigits, ToStringStyle.JSON_STYLE);
            System.out.println("ToStrJson:" + result);
            return result;
        }
    }

    static class IsDigits {
        /**
         * 字符串
         */
        private final String str;
        /**
         * str中包含的数字
         */
        private final String digit;

        public IsDigits(String str, String digit) {
            this.str = str;
            this.digit = digit;
        }

        public static IsDigits parse(String str) {
            //字符串中提取数据
            return new IsDigits(str, StringUtils.getDigits(str));
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
        }
    }

    private static void delay() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static String apply() {
        delay();
        return RandomStringUtils.randomAlphanumeric(5);
    }

    /**
     * testMap 和testCompletableFuture 效果一样，为了测试 map和 CompletableFuture (thenCompose)
     * <p>
     * thenCompose允许将两个异步操作进行流水线，第一个操作完成时,将其结果作为参数传递给第二个操作。
     * 换句话说，可以创建两个两个CompletableFutre对象，对第一个CompletableFuture调用
     * thenCompose，并向其传递一个函数，
     * </p>
     */
    public static void testMap() {
        final List<String> collect = IntStream.rangeClosed(1, 10).mapToObj(i -> {
            final String apply = apply();
            System.out.println("生成的随机字符串:" + apply);
            return apply;
        }).map(IsDigits::parse)//字符串转换成 IsDigits
                .map(ToStrJson::toJson)// IsDigits 转换 json字符串
                .collect(toList());
    }

    public static void testCompletableFuture() {
        final List<CompletableFuture<String>> collect = IntStream.rangeClosed(1,10).mapToObj(i ->
                CompletableFuture.supplyAsync(() -> {
                            final String apply = apply();
                            System.out.println("生成的随机字符串:" + apply);
                            return apply;
                        }
                )
        ).map(future -> future.thenApply(IsDigits::parse))
                .map(future -> future.thenCompose(isDigits ->
                        CompletableFuture.supplyAsync(
                                () -> ToStrJson.toJson(isDigits))))
                .collect(toList());
        //等待 执行完毕
        collect.stream().map(CompletableFuture::join).collect(toList());
    }

    public static void main(String[] args) throws InterruptedException {
        final long start = System.nanoTime();
        testCompletableFuture();
        System.out.println();
        System.out.println("执行时间:" + (System.nanoTime() - start) / 1_000_000);
    }
}
