package com.deduplication.read;

import com.deduplication.bloomfilter.BloomFilter;
import com.deduplication.cache.ReadCache;
import com.deduplication.container.ContainerManager;
import com.deduplication.store.SegmentIndexStore;

public class Reader {

	private ReadCache readCache;
	private BloomFilter<String> bloomFilter;
	private SegmentIndexStore segmentIndexStore;
	private ContainerManager containerManager;

	public Reader(ReadCache readCache, BloomFilter<String> bloomFilter,
			SegmentIndexStore segmentIndexStore,
			ContainerManager containerManager) {
		this.readCache = readCache;
		this.bloomFilter = bloomFilter;
		this.segmentIndexStore = segmentIndexStore;
		this.containerManager = containerManager;
	}

	public byte[] get(String hash) {

		byte[] resultData = null;
		// get from read cache
		if ((resultData = getDataFromReadCache(hash)) != null) {
		//	System.out.println("Reader: cache hit");
			return resultData;
		}

		// check in current container Index
		if (containerManager.isHashInCurrentContainer(hash)) {
		//	System.out.println("Reader: hash in current container");
			containerManager.addCurrentContainerIntoReadCache();
			return readCache.get(hash);
		}

		// check in bloom filter
		if (checkBloomFilter(hash)) {
		//	System.out.println("Reader: BloomFilter positive");
			// check segment Index and add data and metadata into read cache
			Long containerId = segmentIndexStore.get(hash);
			if (containerId == null) {
		//		System.out.println("Reader: not in segment index");
				return null;
			} else {
		//		System.out.println("Reader: in segment index");
				containerManager
						.addContainerDataAndMetaIntoReadCache(containerId);
				return readCache.get(hash);
			}

		} else {
		//	System.out.println("Reader: BloomFilter negative");
			return null;
		}

	}

	private byte[] getDataFromReadCache(String hash) {
		return readCache.get(hash);
	}

	private boolean checkBloomFilter(String hash) {
		return bloomFilter.contains(hash);
	}

}