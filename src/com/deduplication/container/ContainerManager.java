package com.deduplication.container;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.deduplication.bloomfilter.BloomFilter;
import com.deduplication.cache.ReadCache;
import com.deduplication.cache.WriteCache;
import com.deduplication.store.ContainerMetadataStore;
import com.deduplication.store.FileContainerStore;
import com.deduplication.store.SegmentIndexStore;
import com.deduplication.store.ContainerMetadataStore.SegmentMetadata;
import com.deduplication.store.ContainerStore;

public class ContainerManager {

	public static final int CONTAINER_LENGTH = 104857600;
	private long currentContainerId;
	private byte[] currentDataContainer;
	private List<SegmentMetadata> currentMetadataContainer;
	private ContainerMetadataStore containerMetadataStore;
	private ContainerStore containerStore;
	private SegmentIndexStore segmentIndexStore;
	private Set<String> currentContainerIndex;
	private WriteCache writeCache;
	private ReadCache readCache;
	private BloomFilter<String> bloomFilter;
	private FileContainerStore fileContainerStore;
	private boolean isFileContainerStore;
	private int currentContainerSize;

	public ContainerManager(WriteCache writeCache, ReadCache readCache,
			ContainerMetadataStore containerMetadataStore,
			ContainerStore containerStore, SegmentIndexStore segmentIndexStore,
			BloomFilter<String> bloomFilter, boolean isFileContainerStore,
			FileContainerStore fileContainerStore) {

		this.writeCache = writeCache;
		this.readCache = readCache;
		this.containerMetadataStore = containerMetadataStore;
		this.containerStore = containerStore;
		this.segmentIndexStore = segmentIndexStore;
		this.bloomFilter = bloomFilter;
		this.isFileContainerStore = isFileContainerStore;
		this.fileContainerStore = fileContainerStore;

		currentContainerId = 1;
		currentContainerSize = 0;
		currentDataContainer = new byte[CONTAINER_LENGTH];
		currentMetadataContainer = new ArrayList<SegmentMetadata>();
		currentContainerIndex = new HashSet<String>();
	}

	public void addIntoContainer(String hash, byte[] data, int dataLength) {

		System.out.println("ContainerManager: Add into container");
		if (currentContainerSize + dataLength <= CONTAINER_LENGTH) {

			System.out.println("ContainerManager: Add into current container");
			currentMetadataContainer.add(new SegmentMetadata(hash, dataLength));
			currentContainerIndex.add(hash);
			for (byte eachByte : data) {
				currentDataContainer[currentContainerSize] = eachByte;
				currentContainerSize++;
			}

		} else {
			System.out.println("ContainerManager: Add into new container");
			persistDataContainer(currentContainerId, currentDataContainer);
			persistMetadataContainer(currentContainerId,
					currentMetadataContainer);
			persistSegmentIndex(currentContainerId, currentMetadataContainer);

			currentContainerId++;
			currentContainerSize = 0;
			currentMetadataContainer.clear();
			currentContainerIndex.clear();
			currentMetadataContainer.add(new SegmentMetadata(hash, dataLength));
			currentContainerIndex.add(hash);
			for (byte eachByte : data) {
				currentDataContainer[currentContainerSize] = eachByte;
				currentContainerSize++;
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
		byte[] containerBytes;
		if (isFileContainerStore) {
			containerBytes = fileContainerStore.get(containerId);
		} else {
			containerBytes = containerStore.get(containerId);
		}

		int iterIndex = 0;

		while (iterMetadata.hasNext()) {
			SegmentMetadata currentMetadata = iterMetadata.next();
			byte[] currentData = new byte[currentMetadata.length];

			for (int i = 0; i < currentMetadata.length; i++) {
				currentData[i] = containerBytes[iterIndex];
				iterIndex++;
			}

			readCache.set(currentMetadata.hash, currentData);
		}
	}

	public void addCurrentContainerIntoReadCache() {

		Iterator<SegmentMetadata> iterMetadata = currentMetadataContainer
				.iterator();
		int iterIndex = 0;

		while (iterMetadata.hasNext()) {
			SegmentMetadata currentMetadata = iterMetadata.next();
			byte[] currentData = new byte[currentMetadata.length];

			for (int i = 0; i < currentMetadata.length; i++) {
				currentData[i] = currentDataContainer[iterIndex];
				iterIndex++;
			}

			readCache.set(currentMetadata.hash, currentData);
		}
	}

	private void persistDataContainer(long containerId, byte[] byteContentList) {
		System.out
				.println("ContainerManager: Persist Data Container with containerId "
						+ containerId);
		try {
			if (isFileContainerStore) {
				fileContainerStore.put(containerId, byteContentList);
			} else {
				containerStore.put(containerId, byteContentList);
			}
			System.out.println("Persisted container successfully");
		} catch (Exception e) {
			System.out.println(e);
		}

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
