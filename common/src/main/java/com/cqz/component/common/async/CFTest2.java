package com.cqz.component.common.async;

import com.cqz.component.common.utils.ExceptionUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *  CF多元依赖测试
 */
public class CFTest2 {
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
        CompletableFuture<String> cf3 = CompletableFuture.supplyAsync(() -> {
            System.out.println("step 3");
            return "step 3 result";
        }, executor);

        CompletableFuture<Void> cf = CompletableFuture.allOf(cf1, cf2, cf3);
        CompletableFuture<String> future = cf.thenApply(v -> {
            String r1 = cf1.join();
            String r2 = cf2.join();
            String r3 = cf3.join();
            throw new RuntimeException("errrrrrrrr");
//            return "result";
        });

        future.thenAccept(System.out::println).exceptionally(err->{
            System.err.println("error!!!"+ ExceptionUtils.extractRealException(err));
            return null;
            });

        executor.shutdown();

    }
}
