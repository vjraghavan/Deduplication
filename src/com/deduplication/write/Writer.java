package com.deduplication.write;

import java.util.ArrayList;
import java.util.List;

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
	private boolean isLocalityCache;
	public long totalCacheHits;
	public long totalCacheMiss;
	public int maxCacheHitLength;
	public int currentCacheHitLength;
	public List<Integer> cacheHitLengthList;

	public Writer(WriteCache writeCache, BloomFilter<String> bloomFilter,
			SegmentIndexStore segmentIndexStore,
			ContainerManager containerManager, boolean isLocalityCache) {
		this.writeCache = writeCache;
		this.bloomFilter = bloomFilter;
		this.segmentIndexStore = segmentIndexStore;
		this.containerManager = containerManager;
		this.numReadDiskSegmentIndex = 0;
		this.totalCacheHits = 0;
		this.totalCacheMiss = 0;
		this.currentCacheHitLength = 0;
		this.maxCacheHitLength = 0;
		this.isLocalityCache = isLocalityCache;
		this.cacheHitLengthList = new ArrayList<Integer>();
	}

	public void put(String hash, byte[] data, int dataLength) {

		// check in cache
		if (checkWriteCache(hash)) {
		//	System.out.println("Writer: cache hit");
			totalCacheHits++;
			currentCacheHitLength++;
			return;
		}
    
		cacheHitLengthList.add(currentCacheHitLength);
		
		if(currentCacheHitLength > maxCacheHitLength)
			maxCacheHitLength = currentCacheHitLength;
		
		currentCacheHitLength = 0;
		totalCacheMiss++;
		
		// check in current container Index
		if (containerManager.isHashInCurrentContainer(hash)) {
			return;
		}

		// check in bloom filter
		if (checkBloomFilter(hash)) {
			// check segment Index and add it to container if not
			// present in it.
			Long containerId = segmentIndexStore.get(hash);
			numReadDiskSegmentIndex++;
			if (containerId == null) {
		        //System.out.println("Writer: not in segment index");
				containerManager.addIntoContainer(hash, data, dataLength);
				return;
			} else {
			    //System.out.println("Writer: in segment index");
				if(isLocalityCache)
					containerManager.addContainerMetadataIntoCache(containerId);
				else
					containerManager.addJustCurrentHashIntoCache(hash, containerId);
				return;
			}

		} else {
		    //System.out.println("Writer: BloomFilter negative");
			containerManager.addIntoContainer(hash, data, dataLength);
			return;
		}
    
	}

	private boolean checkWriteCache(String hash) {
		return (writeCache.get(hash) != null ? true : false);
	}

	private boolean checkBloomFilter(String hash) {
		return bloomFilter.contains(hash);
	}
	
}