package com.deduplication.container;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.deduplication.bloomfilter.BloomFilter;
import com.deduplication.cache.ReadCache;
import com.deduplication.cache.WriteCache;
import com.deduplication.store.ContainerMetadataStore;
import com.deduplication.store.SegmentIndexStore;
import com.deduplication.store.ContainerMetadataStore.SegmentMetadata;
import com.deduplication.store.ContainerStore;

public class ContainerManager {

	public static final int CONTAINER_LENGTH = 4;
	private long currentContainerId;
	private List<Byte> currentDataContainer;
	private List<SegmentMetadata> currentMetadataContainer;
	private ContainerMetadataStore containerMetadataStore;
	private ContainerStore containerStore;
	private SegmentIndexStore segmentIndexStore;
	private Set<String> currentContainerIndex;
	private WriteCache writeCache;
	private ReadCache readCache;
	private BloomFilter<String> bloomFilter;

	public ContainerManager(WriteCache writeCache, ReadCache readCache,
			ContainerMetadataStore containerMetadataStore,
			ContainerStore containerStore, SegmentIndexStore segmentIndexStore,
			BloomFilter<String> bloomFilter) {

		this.writeCache = writeCache;
		this.readCache = readCache;
		this.containerMetadataStore = containerMetadataStore;
		this.containerStore = containerStore;
		this.segmentIndexStore = segmentIndexStore;
		this.bloomFilter = bloomFilter;

		currentContainerId = 1;
		currentDataContainer = new ArrayList<Byte>();
		currentMetadataContainer = new ArrayList<SegmentMetadata>();
		currentContainerIndex = new HashSet<String>();
	}

	public void addIntoContainer(String hash, byte[] data, int dataLength) {

		System.out.println("ContainerManager: Add into container");
		if (currentDataContainer.size() + dataLength < CONTAINER_LENGTH) {

			System.out.println("ContainerManager: Add into current container");
			currentMetadataContainer.add(new SegmentMetadata(hash,
					currentDataContainer.size(), dataLength));
			currentContainerIndex.add(hash);
			for (byte eachByte : data) {
				currentDataContainer.add(eachByte);
			}

		} else {
			System.out.println("ContainerManager: Add into new container");
			persistDataContainer(currentContainerId, currentDataContainer);
			persistMetadataContainer(currentContainerId,
					currentMetadataContainer);
			persistSegmentIndex(currentContainerId, currentMetadataContainer);

			currentContainerId++;
			currentDataContainer = new ArrayList<Byte>();
			currentMetadataContainer = new ArrayList<SegmentMetadata>();
			currentContainerIndex = new HashSet<String>();

			currentMetadataContainer.add(new SegmentMetadata(hash,
					currentDataContainer.size(), dataLength));
			currentContainerIndex.add(hash);
			for (byte eachByte : data) {
				currentDataContainer.add(eachByte);
			}
		}

	}

	public boolean isHashInCurrentContainer(String hash) {
		return currentContainerIndex.contains(hash);
	}

	public void addContainerMetadataIntoCache(Long containerId) {

		Iterator<SegmentMetadata> iter = containerMetadataStore
				.get(containerId).iterator();
		while (iter.hasNext()) {
			writeCache.set(iter.next().hash, containerId);
		}
	}

	public void addContainerDataAndMetaIntoReadCache(Long containerId) {

		Iterator<SegmentMetadata> iterMetadata = containerMetadataStore.get(
				containerId).iterator();
		List<Byte> containerBytes = containerStore.get(containerId);
		Iterator<Byte> iterData = containerBytes.iterator();

		while (iterMetadata.hasNext()) {
			SegmentMetadata currentMetadata = iterMetadata.next();
			byte[] currentData = new byte[currentMetadata.length];

			for (int i = 0; i < currentMetadata.length; i++) {
				currentData[i] = iterData.next();
			}

			readCache.set(currentMetadata.hash, currentData);
		}
	}

	public void addCurrentContainerIntoReadCache() {

		Iterator<SegmentMetadata> iterMetadata = currentMetadataContainer.iterator();
		Iterator<Byte> iterData = currentDataContainer.iterator();
		
		while (iterMetadata.hasNext()) {
			SegmentMetadata currentMetadata = iterMetadata.next();
			byte[] currentData = new byte[currentMetadata.length];

			for (int i = 0; i < currentMetadata.length; i++) {
				currentData[i] = iterData.next();
			}

			readCache.set(currentMetadata.hash, currentData);
		}
	}

	private void persistDataContainer(long containerId,
			List<Byte> byteContentList) {
		System.out
				.println("ContainerManager: Persist Data Container with containerId "
						+ containerId);
		containerStore.put(containerId, byteContentList);
	}

	private void persistMetadataContainer(long currentContainerId,
			List<SegmentMetadata> currentMetadataContainer) {
		System.out
				.println("ContainerManager: Persist MetaData Container with containerId "
						+ currentContainerId);
		containerMetadataStore
				.put(currentContainerId, currentMetadataContainer);
	}

	private void persistSegmentIndex(long currentContainerId,
			List<SegmentMetadata> currentMetadataContainer) {

		System.out
				.println("ContainerManager: Persist Segment Index with containerId "
						+ currentContainerId);

		Iterator<SegmentMetadata> iter = currentMetadataContainer.iterator();
		while (iter.hasNext()) {
			String hash = iter.next().hash;
			segmentIndexStore.put(hash, currentContainerId);
			bloomFilter.add(hash);
		}
	}

}
