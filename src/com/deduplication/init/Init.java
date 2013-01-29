package com.deduplication.init;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
				containerMetadataStore, containerStore, segmentIndexStore,
				bloomFilter);
		Writer writer = new Writer(cache, bloomFilter, segmentIndexStore,
				containerManager);

		byte[] data1 = new byte[3];
		byte[] data2 = new byte[2];
		byte[] data3 = new byte[1];
		byte[] data4 = new byte[2];

		for (int i = 0; i < data1.length; i++)
			data1[i] = ((byte) i);
		for (int i = 0; i < data2.length; i++)
			data2[i] = ((byte) i);
		for (int i = 0; i < data3.length; i++)
			data3[i] = ((byte) i);
		for (int i = 0; i < data4.length; i++)
			data4[i] = ((byte) i);

		writer.put("data1", data1, data1.length);
		writer.put("data2", data2, data2.length);
		writer.put("data3", data3, data3.length);
		writer.put("data4", data4, data4.length);

		writer.put("data1", data1, data1.length);
		writer.put("data2", data2, data2.length);
		writer.put("data3", data3, data3.length);
		writer.put("data4", data4, data4.length);

		System.out.println(cache.get("data1"));

		System.out.println(containerMetadataStore.get(new Long(1)));
		System.out.println(containerMetadataStore.get(new Long(25)));
		containerMetadataStore.close();
		containerStore.close();
		segmentIndexStore.close();
	}
}