package com.deduplication.write;

import java.util.List;

import com.deduplication.bloomfilter.BloomFilter;
import com.deduplication.cache.Cache;
import com.deduplication.container.ContainerManager;
import com.deduplication.store.SegmentIndexStore;

public class Writer {

	private Cache cache;
	private BloomFilter bloomFilter;
	private SegmentIndexStore segmentIndexStore;
	private ContainerManager containerManager;

	public Writer(Cache cache, BloomFilter bloomFilter,
			SegmentIndexStore segmentIndexStore,
			ContainerManager containerManager) {
		this.cache = cache;
		this.bloomFilter = bloomFilter;
		this.segmentIndexStore = segmentIndexStore;
		this.containerManager = containerManager;
	}

	void put(String hash, byte[] data, int dataLength) {
		
		// check in cache
		if (checkCache(hash)) {
			return;
		}

		// check in current container Index
		if (containerManager.isIndexInCurrentContainer(hash)) {
			return;
		}
		
		// check in bloom filter
		if (checkBloomFilter(hash)) {
			// check segment Index and add it to container if not
			// present in it.
			String containerId = segmentIndexStore.get(hash);
			if (containerId == null) {
				containerManager.addIntoContainer(hash, data, dataLength);
			} else {
				containerManager.addContainerMetadataIntoCache(containerId);
				return;
			}

		} else {
			containerManager.addIntoContainer(hash, data, dataLength);
		}

	}

	private boolean checkCache(String hash) {
		return (cache.get(hash) != null ? true : false);
	}

	private boolean checkBloomFilter(String hash) {
		return bloomFilter.contains(hash);
	}

	public static void main(String args[]) {
		BloomFilter bf = new BloomFilter(0.001, Integer.MAX_VALUE);
		bf.add(new String("vijay"));
		System.out.println(bf.contains(new String("vijay")));
		System.out.println(bf.contains(new String("prakash")));
		System.out.println(Integer.MAX_VALUE);
	}

}