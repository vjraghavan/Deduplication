package com.deduplication.write;

import java.util.List;

import com.deduplication.bloomfilter.BloomFilter;
import com.deduplication.cache.Cache;

public class Writer {
	
	private BloomFilter bf;
	private Cache cache;
	
	public Writer(BloomFilter bf, Cache cache){
		this.bf = bf;
		this.cache = cache;
	}
	
	void put(String hash, byte[] data){		
	}
	
	private boolean checkCache(String hash){
		return true;
	}
	
	private boolean checkBloomFilter(String hash){
		return true;
	}
	
	private void addIntoContainer(String hash, byte[] data){
		
	}
	
	private void persistContainer(String containerId, List<Byte> dataBytes){
		
	}
	
	public static void main(String args[]){
		BloomFilter bf = new BloomFilter(0.001, Integer.MAX_VALUE);
		bf.add(new String("vijay"));
		System.out.println(bf.contains(new String("vijay")));
		System.out.println(bf.contains(new String("prakash")));
		System.out.println(Integer.MAX_VALUE);
	}
	
}