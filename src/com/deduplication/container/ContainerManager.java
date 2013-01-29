package com.deduplication.container;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.deduplication.bloomfilter.BloomFilter;
import com.deduplication.cache.Cache;
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
	private Cache cache;
	private BloomFilter<String> bloomFilter;

	public ContainerManager(Cache cache,
			ContainerMetadataStore containerMetadataStore,
			ContainerStore containerStore, SegmentIndexStore segmentIndexStore,
			BloomFilter<String> bloomFilter) {

		this.cache = cache;
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

	public boolean isIndexInCurrentContainer(String hash) {
		return currentContainerIndex.contains(hash);
	}

	public void addContainerMetadataIntoCache(Long containerId) {

		Iterator<SegmentMetadata> iter = containerMetadataStore
				.get(containerId).iterator();
		while (iter.hasNext()) {
			cache.set(iter.next().hash, containerId);
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
