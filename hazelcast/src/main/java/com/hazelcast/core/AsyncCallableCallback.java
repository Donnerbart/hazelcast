package com.hazelcast.core;

public interface AsyncCallableCallback<V> {

    public void setResult(V result);
}
