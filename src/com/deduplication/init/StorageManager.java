package com.deduplication.init;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import net.spy.memcached.MemcachedClient;

import com.deduplication.bloomfilter.BloomFilter;
import com.deduplication.cache.ReadCache;
import com.deduplication.cache.WriteCache;
import com.deduplication.container.ContainerManager;
import com.deduplication.read.Reader;
import com.deduplication.store.ContainerMetadataStore;
import com.deduplication.store.ContainerMetadataStore.SegmentMetadata;
import com.deduplication.store.ContainerStore;
import com.deduplication.store.FileContainerStore;
import com.deduplication.store.SegmentIndexStore;
import com.deduplication.write.Writer;
import com.sleepycat.je.EnvironmentConfig;

public class StorageManager {

	private EnvironmentConfig envConfig;
	private FileContainerStore fileContainerStore;
	private ContainerStore containerStore;
	private ContainerMetadataStore containerMetadataStore;
	private ContainerManager containerManager;

	private SegmentIndexStore segmentIndexStore;
	private WriteCache writeCache;
	private ReadCache readCache;
	private BloomFilter<String> bloomFilter;
	private Writer writer;
	private Reader reader;
	private boolean isFileContainerStore;

	public StorageManager() {
		try {
			isFileContainerStore = true;
			envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setTransactional(true);
			envConfig.setSharedCacheVoid(true);
			if (!isFileContainerStore) {
				envConfig.setCachePercentVoid(75);
			}
			envConfig.setTxnNoSyncVoid(true);

			fileContainerStore = new FileContainerStore(
					"/home/vijay/Archive/FileContainerStore/");

			containerStore = new ContainerStore(new File(
					"/home/vijay/Archive/ContainerStore"), envConfig);
			containerMetadataStore = new ContainerMetadataStore(new File(
					"/home/vijay/Archive/ContainerMetadataStore"), envConfig);
			segmentIndexStore = new SegmentIndexStore(new File(
					"/home/vijay/Archive/SegmentIndexStore"), envConfig);
			writeCache = new WriteCache();
			readCache = new ReadCache();
			bloomFilter = new BloomFilter<String>(0.001, Integer.MAX_VALUE);
			containerManager = new ContainerManager(writeCache, readCache,
					containerMetadataStore, containerStore, segmentIndexStore,
					bloomFilter, isFileContainerStore, fileContainerStore);
			writer = new Writer(writeCache, bloomFilter, segmentIndexStore,
					containerManager);
			reader = new Reader(readCache, bloomFilter, segmentIndexStore,
					containerManager);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public StorageManager(ContainerStore containerStore,
			ContainerMetadataStore containerMetadataStore,
			ContainerManager containerManager,
			SegmentIndexStore segmentIndexStore, WriteCache writeCache,
			ReadCache readCache, BloomFilter<String> bloomFilter,
			Writer writer, Reader reader) {

		this.containerStore = containerStore;
		this.containerMetadataStore = containerMetadataStore;
		this.containerManager = containerManager;
		this.segmentIndexStore = segmentIndexStore;
		this.writeCache = writeCache;
		this.readCache = readCache;
		this.bloomFilter = bloomFilter;
		this.writer = writer;
		this.reader = reader;
	}

	public void put(byte[] hash, byte[] data)
			throws UnsupportedEncodingException {
		String strHash = convertToHex(hash);
		writer.put(strHash, data, data.length);
	}

	public byte[] get(byte[] hash) {
		String strHash = convertToHex(hash);
		return reader.get(strHash);
	}

	public static void main(String args[]) throws IOException {

		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		envConfig.setSharedCacheVoid(true);
		envConfig.setCachePercentVoid(75);
		envConfig.setTxnNoSyncVoid(true);

		ContainerStore containerStore = new ContainerStore(new File(
				"/home/vijay/Archive/ContainerStore"), envConfig);
		ContainerMetadataStore containerMetadataStore = new ContainerMetadataStore(
				new File("/home/vijay/Archive/ContainerMetadataStore"),
				envConfig);
		SegmentIndexStore segmentIndexStore = new SegmentIndexStore(new File(
				"/home/vijay/Archive/SegmentIndexStore"), envConfig);
		WriteCache writeCache = new WriteCache();
		ReadCache readCache = new ReadCache();
		BloomFilter<String> bloomFilter = new BloomFilter<String>(0.001,
				Integer.MAX_VALUE);
		ContainerManager containerManager = new ContainerManager(writeCache,
				readCache, containerMetadataStore, containerStore,
				segmentIndexStore, bloomFilter, false, null);
		Writer writer = new Writer(writeCache, bloomFilter, segmentIndexStore,
				containerManager);
		Reader reader = new Reader(readCache, bloomFilter, segmentIndexStore,
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

		writer.put("data5", data1, data1.length);
		writer.put("data6", data2, data2.length);
		writer.put("data7", data3, data3.length);
		writer.put("data8", data4, data4.length);

		writer.put("data5", data1, data1.length);
		writer.put("data6", data2, data2.length);
		writer.put("data7", data3, data3.length);
		writer.put("data8", data4, data4.length);

		byte[] data5 = reader.get("data5");
		System.out.println("Data for the key data5");
		for (byte b : data5) {
			System.out.print(b + " ");
		}

		byte[] data6 = reader.get("data6");
		System.out.println("Data for the key data6");
		for (byte b : data6) {
			System.out.print(b + " ");
		}

		byte[] data7 = reader.get("data7");
		System.out.println("Data for the key data7");
		for (byte b : data7) {
			System.out.print(b + " ");
		}

		byte[] data8 = reader.get("data8");
		System.out.println("Data for the key data8");
		for (byte b : data8) {
			System.out.print(b + " ");
		}

		byte[] data6s = reader.get("data6");
		System.out.println("Again Data for the key data6");
		for (byte b : data6s) {
			System.out.print(b + " ");
		}

		containerMetadataStore.close();
		containerStore.close();
		segmentIndexStore.close();
	}

	private static String convertToHex(byte[] data) {
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < data.length; i++) {
			int halfbyte = (data[i] >>> 4) & 0x0F;
			int two_halfs = 0;
			do {
				if ((0 <= halfbyte) && (halfbyte <= 9))
					buf.append((char) ('0' + halfbyte));
				else
					buf.append((char) ('a' + (halfbyte - 10)));
				halfbyte = data[i] & 0x0F;
			} while (two_halfs++ < 1);
		}
		return buf.toString();
	}
	
	public long numDiskReadsSegmentIndex(){
		return writer.numReadDiskSegmentIndex;
	}
}
