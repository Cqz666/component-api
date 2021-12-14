package com.cqz.component.common.async;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

public class ThenComposeTest {
    public static void main(String[] args) throws Exception {
        long s = System.nanoTime();
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            long start = System.nanoTime();
            delay();
            String a = getValue("a");
            printCurrentTime(a);
            System.out.println("Done in "+(System.nanoTime()-start)/1_000_000+" ms");
            return a;
        }).thenCompose(param-> CompletableFuture.supplyAsync(()->{
            long start = System.nanoTime();
            delay();
            String b = getValue(param)+"+b";
            printCurrentTime(b);
            System.out.println("Done in "+(System.nanoTime()-start)/1_000_000+" ms");

            return b;
        }));
        System.out.println("thenCompose result : " + future.get());
        System.out.println("Done in "+(System.nanoTime()-s)/1_000_000+" ms");

    }

    public static String getValue(String val){
        return val;
    }

    public static void delay() {
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void printCurrentTime(String key){
        System.out.println(key+"："+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
    }


}
