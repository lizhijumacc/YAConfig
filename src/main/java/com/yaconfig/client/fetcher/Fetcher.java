package com.yaconfig.client.fetcher;

public interface Fetcher {
	public void fetch(String location,FetchCallback callback);
}
