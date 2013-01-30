package com.deduplication.cache;
import java.io.IOException;
import java.net.InetSocketAddress;
import net.spy.memcached.MemcachedClient;

public class ReadCache {
	
	private MemcachedClient readCache;
	
	public ReadCache(){
		try {
			readCache = new MemcachedClient(
				    new InetSocketAddress("localhost", 1122));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void set(String key, Object value){
		readCache.set(key, 3600, value);
	}
	
	public byte[] get(String key){
		return (byte[])readCache.get(key);
	}
}
