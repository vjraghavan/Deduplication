package com.deduplication.write;

import java.util.List;

import com.deduplication.bloomfilter.BloomFilter;
import com.deduplication.cache.Cache;
import com.deduplication.container.ContainerManager;

public class Writer {
	
	private BloomFilter bloomFilter;
	private Cache cache;
	private ContainerManager containerManager;
	
	public Writer(BloomFilter bloomFilter, Cache cache, ContainerManager containerManager){
		this.bloomFilter = bloomFilter;
		this.cache = cache;
		this.containerManager = containerManager;
	}
	
	void put(String hash, byte[] data, int dataLength){		
		if(checkCache(hash)){
			return;
		}else{
			if(checkBloomFilter(hash)){
				// TODO : check segment Index and add it to container if not present in it.
			}else{
				containerManager.addIntoContainer(hash, data, dataLength);
			}
		}
	}
	
	private boolean checkCache(String hash){
		return (cache.get(hash) != null ? true : false);
	}
	
	private boolean checkBloomFilter(String hash){
		return bloomFilter.contains(hash);
	}
	
	public static void main(String args[]){
		BloomFilter bf = new BloomFilter(0.001, Integer.MAX_VALUE);
		bf.add(new String("vijay"));
		System.out.println(bf.contains(new String("vijay")));
		System.out.println(bf.contains(new String("prakash")));
		System.out.println(Integer.MAX_VALUE);
	}
	
}