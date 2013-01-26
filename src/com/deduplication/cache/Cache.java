package com.deduplication.cache;
import java.io.IOException;
import java.net.InetSocketAddress;
import net.spy.memcached.MemcachedClient;

public class Cache {
	
	private MemcachedClient cache;
	
	public Cache(){
		try {
			cache = new MemcachedClient(
				    new InetSocketAddress("localhost", 1121));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void set(String key, Object value){
		cache.set(key, 3600, value);
	}
	
	public Object get(String key){
		return cache.get(key);
	}

	public static void main(String args[]){
		Cache memCache = new Cache();
		memCache.set("random1", 5);
		System.out.println(memCache.get("random1"));
	}
}
