package com.deduplication.container;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.deduplication.store.ContainerMetadataStore;
import com.deduplication.store.ContainerMetadataStore.SegmentMetadata;
import com.deduplication.store.ContainerStore;

public class ContainerManager {

	public static final int CONTAINER_LENGTH = 1000000;
	private String currentContainerId;
	private List<Byte> currentDataContainer;
	private List<SegmentMetadata> currentMetadataContainer;
	private ContainerMetadataStore containerMetadataStore;
	private ContainerStore containerStore;

	public ContainerManager(ContainerMetadataStore containerMetadataStore,
			ContainerStore containerStore) {

		this.containerMetadataStore = containerMetadataStore;
		this.containerStore = containerStore;
		currentContainerId = UUID.randomUUID().toString();
		currentDataContainer = new ArrayList<Byte>();
		currentMetadataContainer = new ArrayList<SegmentMetadata>();
	}

	public void addIntoContainer(String hash, byte[] data, int dataLength) {

		if (currentDataContainer.size() + dataLength < CONTAINER_LENGTH) {

			currentMetadataContainer.add(new SegmentMetadata(hash,
					currentDataContainer.size(), dataLength));
			for (byte eachByte : data) {
				currentDataContainer.add(eachByte);
			}

		} else {
			persistDataContainer(currentContainerId, currentDataContainer);
			persistMetadataContainer(currentContainerId,
					currentMetadataContainer);
		}
	}

	private void persistDataContainer(String containerId, List<Byte> byteContentList) {
		containerStore.put(containerId, byteContentList);
	}

	private void persistMetadataContainer(String currentContainerId,
			List<SegmentMetadata> currentMetadataContainer) {
		containerMetadataStore.put(currentContainerId, currentMetadataContainer);	
	}

}
