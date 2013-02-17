package com.deduplication.cache;

import java.io.IOException;
import java.net.InetSocketAddress;
import net.spy.memcached.MemcachedClient;

public class WriteCache {

	private MemcachedClient cache;

	public WriteCache() {
		try {
			cache = new MemcachedClient(
					new InetSocketAddress("localhost", 1121));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void set(String key, Object value) {
		cache.set(key, 3600 * 24, value);
	}

	public Object get(String key) {
		return cache.get(key);
	}
}
