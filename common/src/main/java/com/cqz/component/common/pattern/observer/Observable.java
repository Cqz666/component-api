package com.cqz.component.common.pattern.observer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Observable {

    private final Map<Object, Listener> listeners = new ConcurrentHashMap<>();

    public void register(Object key, Listener listener) {
        listeners.put(key, listener);
    }
    public void unregister(Object key) {
        listeners.remove(key);
    }

    public void sendEvent(Object event) {
        for (Listener listener : listeners.values()) {
            listener.onEvent( event );
        }
    }

}
