package com.hazelcast.core;

import java.util.concurrent.Callable;

public abstract class AsyncCallable<V> implements Callable<V> {

    private volatile transient AsyncCallableCallback<V> callback;

    @Override
    public final V call() throws Exception {
        asyncCall();

        return null;
    }

    protected abstract void asyncCall() throws Exception;

    protected final void setResult(V result) {
        callback.setResult(result);
    }

    public final void setCallback(AsyncCallableCallback<V> callback) {
        this.callback = callback;
    }
}
