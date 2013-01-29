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

	public void get(String hash) {
		
		// check in cache
		if (checkReadCache(hash)) {
			System.out.println("Writer: cache hit");
			return;
		}

		// check in current container Index
		if (containerManager.isHashInCurrentContainer(hash)) {
			System.out.println("Writer: hash in current container");
			return;
		}
		
		// check in bloom filter
		if (checkBloomFilter(hash)) {
			System.out.println("Writer: BloomFilter positive");
			// check segment Index and add it to container if not
			// present in it.
			Long containerId = segmentIndexStore.get(hash);
			if (containerId == null) {
				System.out.println("Writer: not in segment index");
		//		containerManager.addIntoContainer(hash, data, dataLength);
			} else {
				System.out.println("Writer: in segment index");
		//		containerManager.addContainerMetadataIntoCache(containerId);
				return;
			}

		} else {
			System.out.println("Writer: BloomFilter negative");
		//	containerManager.addIntoContainer(hash, data, dataLength);
		}

	}

	private boolean checkReadCache(String hash) {
		return (readCache.get(hash) != null ? true : false);
	}

	private boolean checkBloomFilter(String hash) {
		return bloomFilter.contains(hash);
	}

}