package com.cqz.flink.table.factories;

public class Main {
    private static long mark ;
    public static void main(String[] args) throws InterruptedException {
        long nextReadTime = System.currentTimeMillis();
        while (true){
            nextReadTime += 1000;
            long toWaitMs = nextReadTime - System.currentTimeMillis();
            while (toWaitMs > 0) {
                System.out.println(1);
                Thread.sleep(toWaitMs);
                toWaitMs = nextReadTime - System.currentTimeMillis();
            }
        }

    }
}
