package com.deduplication.container;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ContainerManager {

	public static final int CONTAINER_LENGTH = 1000000;
	private String currentContainerId;
	private List<Byte> currentContainer;
	
	public ContainerManager(){
		currentContainerId = UUID.randomUUID().toString();
		currentContainer = new ArrayList<Byte>();
	}
	
	public void addIntoContainer(String hash, byte[] data, int dataLength) {
		
		if(currentContainer.size() + dataLength < CONTAINER_LENGTH){
			
			
		}else{
			
		}
		
	}

	private void persistContainer(String containerId, List<Byte> dataBytes) {

	}

}
