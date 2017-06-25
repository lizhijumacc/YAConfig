package com.yaconfig.client.future;

public interface FutureListener<V> {

	public void operationCompleted(AbstractFuture<V> abstractFuture);

}
