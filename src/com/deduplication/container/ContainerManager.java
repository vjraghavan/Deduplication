package com.deduplication.container;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.deduplication.store.ContainerMetadataStore;
import com.deduplication.store.SegmentIndexStore;
import com.deduplication.store.ContainerMetadataStore.SegmentMetadata;
import com.deduplication.store.ContainerStore;

public class ContainerManager {

	public static final int CONTAINER_LENGTH = 1000000;
	private String currentContainerId;
	private List<Byte> currentDataContainer;
	private List<SegmentMetadata> currentMetadataContainer;
	private ContainerMetadataStore containerMetadataStore;
	private ContainerStore containerStore;
    private SegmentIndexStore segmentIndexStore;
    private Set<String> currentContainerIndex;
	
    public ContainerManager(ContainerMetadataStore containerMetadataStore,
			ContainerStore containerStore, SegmentIndexStore segmentIndexStore) {

		this.containerMetadataStore = containerMetadataStore;
		this.containerStore = containerStore;
		this.segmentIndexStore = segmentIndexStore;
		
		currentContainerId = UUID.randomUUID().toString();
		currentDataContainer = new ArrayList<Byte>();
		currentMetadataContainer = new ArrayList<SegmentMetadata>();
		currentContainerIndex = new HashSet<String>();
	}

	public void addIntoContainer(String hash, byte[] data, int dataLength) {

		if (currentDataContainer.size() + dataLength < CONTAINER_LENGTH) {

			currentMetadataContainer.add(new SegmentMetadata(hash,
					currentDataContainer.size(), dataLength));
			currentContainerIndex.add(hash);
			for (byte eachByte : data) {
				currentDataContainer.add(eachByte);
			}

		} else {
			persistDataContainer(currentContainerId, currentDataContainer);
			persistMetadataContainer(currentContainerId,
					currentMetadataContainer);
			persistSegmentIndex(currentContainerId, currentMetadataContainer);
			
			currentContainerId = UUID.randomUUID().toString();
			currentDataContainer = new ArrayList<Byte>();
			currentMetadataContainer = new ArrayList<SegmentMetadata>();
			currentContainerIndex = new HashSet<String>();
		}
		
	}

	public boolean isIndexInCurrentContainer(String hash){
		return currentContainerIndex.contains(hash);
	}
	private void persistDataContainer(String containerId, List<Byte> byteContentList) {
		containerStore.put(containerId, byteContentList);
	}

	private void persistMetadataContainer(String currentContainerId,
			List<SegmentMetadata> currentMetadataContainer) {
		containerMetadataStore.put(currentContainerId, currentMetadataContainer);	
	}
	
	private void persistSegmentIndex(String currentContainerId,
			List<SegmentMetadata> currentMetadataContainer) {
		
		Iterator<SegmentMetadata> iter = currentMetadataContainer.iterator();
		while(iter.hasNext()){
			segmentIndexStore.put(iter.next().hash, currentContainerId);
		}
	}

}
