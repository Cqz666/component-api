package com.cqz.component.common.async;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.cqz.component.common.async.ThenComposeTest.*;

public class ThenCombineTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        long s = System.nanoTime();
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            long start = System.nanoTime();
            delay();
            String a = getValue("a");
            printCurrentTime(a);
            System.out.println("Done in " + (System.nanoTime() - start) / 1_000_000 + " ms");
            return a;
        }).thenCombine(CompletableFuture.supplyAsync(() -> {
            long start = System.nanoTime();
            delay();
            String b = getValue("b");
            printCurrentTime(b);
            System.out.println("Done in " + (System.nanoTime() - start) / 1_000_000 + " ms");
            return b;
        }), (a, b) -> {
            printCurrentTime("combine");
            return a + "+" + b;
        });
        String combine = future.get();
        System.out.println(combine);
        System.out.println("Done in " + (System.nanoTime() - s) / 1_000_000 + " ms");

    }
}
