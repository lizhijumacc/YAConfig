package com.yaconfig.client;

public interface IFutureListener<V> {

	public void operationCompleted(AbstractFuture<V> abstractFuture);

}
