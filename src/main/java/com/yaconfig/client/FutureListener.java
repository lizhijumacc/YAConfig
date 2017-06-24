package com.yaconfig.client;

public interface FutureListener<V> {

	public void operationCompleted(AbstractFuture<V> abstractFuture);

}
