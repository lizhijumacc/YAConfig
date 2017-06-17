package com.yaconfig.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface IFuture<V> {

	boolean cancel(boolean mayInterruptIfRunning);

	boolean isCancellable();

	boolean isCancelled();

	boolean isDone();

	V get() throws InterruptedException, ExecutionException;

	V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;

	boolean isSuccess();

	V getNow();

	Throwable cause();

	IFuture<V> addListener(IFutureListener<V> listener);

	IFuture<V> removeListener(IFutureListener<V> listener);

	IFuture<V> await() throws InterruptedException;

	boolean await(long timeoutMillis) throws InterruptedException;

	boolean await(long timeout, TimeUnit unit) throws InterruptedException;

	IFuture<V> awaitUninterruptibly();

	boolean awaitUninterruptibly(long timeoutMillis);

	boolean awaitUninterruptibly(long timeout, TimeUnit unit);

}
