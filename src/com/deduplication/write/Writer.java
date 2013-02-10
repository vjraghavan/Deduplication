package com.deduplication.write;

import com.deduplication.bloomfilter.BloomFilter;
import com.deduplication.cache.WriteCache;
import com.deduplication.container.ContainerManager;
import com.deduplication.store.SegmentIndexStore;

public class Writer {

	private WriteCache writeCache;
	private BloomFilter<String> bloomFilter;
	private SegmentIndexStore segmentIndexStore;
	private ContainerManager containerManager;
	public long numReadDiskSegmentIndex;

	public Writer(WriteCache writeCache, BloomFilter<String> bloomFilter,
			SegmentIndexStore segmentIndexStore,
			ContainerManager containerManager) {
		this.writeCache = writeCache;
		this.bloomFilter = bloomFilter;
		this.segmentIndexStore = segmentIndexStore;
		this.containerManager = containerManager;
		this.numReadDiskSegmentIndex = 0;
	}

	public void put(String hash, byte[] data, int dataLength) {

		// check in cache
		/*if (checkWriteCache(hash)) {
		//	System.out.println("Writer: cache hit");
			return;
		}*/
    
		// check in current container Index
		if (containerManager.isHashInCurrentContainer(hash)) {
		//	System.out.println("Writer: hash in current container");
			return;
		}

		// check in bloom filter
	//	if (checkBloomFilter(hash)) {
		//	System.out.println("Writer: BloomFilter positive");
			// check segment Index and add it to container if not
			// present in it.
			Long containerId = segmentIndexStore.get(hash);
			if (containerId == null) {
		//		System.out.println("Writer: not in segment index");
				containerManager.addIntoContainer(hash, data, dataLength);
				numReadDiskSegmentIndex++;
				return;
			} else {
			//	System.out.println("Writer: in segment index");
			//	containerManager.addContainerMetadataIntoCache(containerId);
				return;
			}

	/*	} else {
		//	System.out.println("Writer: BloomFilter negative");
			containerManager.addIntoContainer(hash, data, dataLength);
			return;
		}
    */
	}

	private boolean checkWriteCache(String hash) {
		return (writeCache.get(hash) != null ? true : false);
	}

	private boolean checkBloomFilter(String hash) {
		return bloomFilter.contains(hash);
	}
}