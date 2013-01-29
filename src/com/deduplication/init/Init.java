package com.deduplication.init;

import java.io.File;

import com.deduplication.bloomfilter.BloomFilter;
import com.deduplication.cache.Cache;
import com.deduplication.container.ContainerManager;
import com.deduplication.store.ContainerMetadataStore;
import com.deduplication.store.ContainerStore;
import com.deduplication.store.SegmentIndexStore;
import com.deduplication.write.Writer;

public class Init {

	private ContainerStore containerStore;
	private ContainerMetadataStore containerMetadataStore;
	private ContainerManager containerManager;

	private SegmentIndexStore segmentIndexStore;
	private Cache cache;
	private BloomFilter bloomFilter;
	private Writer writer;

	public Init(ContainerStore containerStore,
			ContainerMetadataStore containerMetadataStore,
			ContainerManager containerManager,
			SegmentIndexStore segmentIndexStore, Cache cache,
			BloomFilter<String> bloomFilter, Writer writer) {

		this.containerStore = containerStore;
		this.containerMetadataStore = containerMetadataStore;
		this.containerManager = containerManager;
		this.segmentIndexStore = segmentIndexStore;
		this.cache = cache;
		this.bloomFilter = bloomFilter;
		this.writer = writer;
	}

	public static void main(String args[]) {
		ContainerStore containerStore = new ContainerStore(new File(
				"/home/vijay/Archive/ContainerStore"));
		ContainerMetadataStore containerMetadataStore = new ContainerMetadataStore(
				new File("/home/vijay/Archive/ContainerMetadataStore"));
		SegmentIndexStore segmentIndexStore = new SegmentIndexStore(new File(
				"/home/vijay/Archive/SegmentIndexStore"));
		Cache cache = new Cache();
		BloomFilter<String> bloomFilter = new BloomFilter<String>(0.001,
				Integer.MAX_VALUE);
		ContainerManager containerManager = new ContainerManager(cache,
				containerMetadataStore, containerStore, segmentIndexStore);
		Writer writer = new Writer(cache, bloomFilter, segmentIndexStore,
				containerManager);
	}
}
