package com.yaconfig.client.future;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AbstractFuture<V> implements Future<V> {

	protected volatile Object result; 
 
	protected Collection<FutureListener<V>> listeners = new CopyOnWriteArrayList<FutureListener<V>>();

	private static final SuccessSignal SUCCESS_SIGNAL = new SuccessSignal();

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		if (isDone()) {
			return false;
		}

		synchronized (this) {
			if (isDone()) { 
				return false;
			}
			result = new CauseHolder(new CancellationException());
			notifyAll();
		}
		notifyListeners();
		return true;
	}
	
	@Override
	public boolean isCancellable() {
		return result == null;
	}
	
	@Override
	public boolean isCancelled() {
		return result != null && result instanceof CauseHolder && ((CauseHolder) result).cause instanceof CancellationException;
	}

	@Override
	public boolean isDone() {
		return result != null;
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		await(); 

		Throwable cause = cause();
		if (cause == null) {
			return getNow();
		}
		if (cause instanceof CancellationException) { 
			throw (CancellationException) cause;
		}
		throw new ExecutionException(cause);
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		if (await(timeout, unit)) {
			Throwable cause = cause();
			if (cause == null) {
				return getNow();
			}
			if (cause instanceof CancellationException) {
				throw (CancellationException) cause;
			}
			throw new ExecutionException(cause);
		}
		
		throw new TimeoutException();
	}

	@Override
	public boolean isSuccess() {
		return result == null ? false : !(result instanceof CauseHolder);
	}

	@SuppressWarnings("unchecked")
	@Override
	public V getNow() {
		return (V) (result == SUCCESS_SIGNAL ? null : result);
	}

	@Override
	public Throwable cause() {
		if (result != null && result instanceof CauseHolder) {
			return ((CauseHolder) result).cause;
		}
		return null;
	}

	@Override
	public Future<V> addListener(FutureListener<V> listener) {
		if (listener == null) {
			throw new NullPointerException("listener");
		}
		if (isDone()) {
			notifyListener(listener);
			return this;
		}
		synchronized (this) {
			if (!isDone()) {
				listeners.add(listener);
				return this;
			}
		}
		notifyListener(listener);
		return this;
	}

	@Override
	public Future<V> removeListener(FutureListener<V> listener) {
		if (listener == null) {
			throw new NullPointerException("listener");
		}

		if (!isDone()) {
			listeners.remove(listener);
		}

		return this;
	}

	@Override
	public Future<V> await() throws InterruptedException {
		return await0(true);
	}

	
	private Future<V> await0(boolean interruptable) throws InterruptedException {
		if (!isDone()) { 
			if (interruptable && Thread.interrupted()) {
				throw new InterruptedException("thread " + Thread.currentThread().getName() + " has been interrupted.");
			}

			boolean interrupted = false;
			synchronized (this) {
				while (!isDone()) {
					try {
						wait(); 
					} catch (InterruptedException e) {
						if (interruptable) {
							throw e;
						} else {
							interrupted = true;
						}
					}
				}
			}
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
		return this;
	}
	
	@Override
	public boolean await(long timeoutMillis) throws InterruptedException {
		return await0(TimeUnit.MILLISECONDS.toNanos(timeoutMillis), true);
	}
	
	@Override
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return await0(unit.toNanos(timeout), true);
	}

	private boolean await0(long timeoutNanos, boolean interruptable) throws InterruptedException {
		if (isDone()) {
			return true;
		}

		if (timeoutNanos <= 0) {
			return isDone();
		}

		if (interruptable && Thread.interrupted()) {
			throw new InterruptedException(toString());
		}

		long startTime = timeoutNanos <= 0 ? 0 : System.nanoTime();
		long waitTime = timeoutNanos;
		boolean interrupted = false;

		try {
			synchronized (this) {
				if (isDone()) {
					return true;
				}

				if (waitTime <= 0) {
					return isDone();
				}

				for (;;) {
					try {
						wait(waitTime / 1000000, (int) (waitTime % 1000000));
					} catch (InterruptedException e) {
						if (interruptable) {
							throw e;
						} else {
							interrupted = true;
						}
					}

					if (isDone()) {
						return true;
					} else {
						waitTime = timeoutNanos - (System.nanoTime() - startTime);
						if (waitTime <= 0) {
							return isDone();
						}
					}
				}
			}
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public Future<V> awaitUninterruptibly() {
		try {
			return await0(false);
		} catch (InterruptedException e) {
			throw new java.lang.InternalError();
		}
	}
	
	@Override
	public boolean awaitUninterruptibly(long timeoutMillis) {
		try {
			return await0(TimeUnit.MILLISECONDS.toNanos(timeoutMillis), false);
		} catch (InterruptedException e) {
			throw new java.lang.InternalError();
		}
	}

	@Override
	public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
		try {
			return await0(unit.toNanos(timeout), false);
		} catch (InterruptedException e) {
			throw new java.lang.InternalError();
		}
	}

	public Future<V> setFailure(Throwable cause) {
		if (setFailure0(cause)) {
			notifyListeners();
			return this;
		}
		return this;
		//throw new IllegalStateException("complete already: " + this);
	}

	private boolean setFailure0(Throwable cause) {
		if (isDone()) {
			return false;
		}

		synchronized (this) {
			if (isDone()) {
				return false;
			}
			result = new CauseHolder(cause);
			notifyAll();
		}

		return true;
	}

	public Future<V> setSuccess(Object result) {
		if (setSuccess0(result)) { 
			notifyListeners();
			return this;
		}
		throw new IllegalStateException("complete already: " + this);
	}

	private boolean setSuccess0(Object result) {
		if (isDone()) {
			return false;
		}

		synchronized (this) {
			if (isDone()) {
				return false;
			}
			if (result == null) { 
				this.result = SUCCESS_SIGNAL;
			} else {
				this.result = result;
			}
			notifyAll();
		}
		return true;
	}

	private void notifyListeners() {
		for (FutureListener<V> l : listeners) {
			notifyListener(l);
		}
	}

	private void notifyListener(FutureListener<V> l) {
		try {
			l.operationCompleted(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static class SuccessSignal {

	}

	private static final class CauseHolder {
		final Throwable cause;

		CauseHolder(Throwable cause) {
			this.cause = cause;
		}
	}
}
