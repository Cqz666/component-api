package com.cqz.component.common.pattern.observer;

public class ObserverMain {
    public static void main(String[] args) {
        Observable observable = new Observable();
        observable.register("key1", System.out::println);
        observable.sendEvent("hello word");
    }
}
