package com.cqz.component.common.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CFTest1 {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        CompletableFuture<String> cf1 = CompletableFuture.supplyAsync(() -> {
            System.out.println("step 1");
            return "step 1 result";
        }, executor);
        CompletableFuture<String> cf2 = CompletableFuture.supplyAsync(() -> {
            System.out.println("step 2");
            return "step 2 result";
        }, executor);

        cf1.thenCombine(cf2,(result1,result2)->{
            System.out.println(result1+" , "+result2);
            System.out.println("step 3");
            return "step 3 result";
        }).thenAccept(System.out::println);

        executor.shutdown();
    }
}
