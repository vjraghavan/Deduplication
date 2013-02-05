package com.deduplication.store;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class FileContainerStore {

	FileInputStream fis;
	ObjectInputStream ois;
	FileOutputStream fos;
	ObjectOutputStream oos;
	String containerDirectory;

	public FileContainerStore(String containerDirectory) {
		this.containerDirectory = containerDirectory;
	}

	public void put(Long containerId, byte[] containerData) {

		try {
			fos = new FileOutputStream(containerDirectory
					+ containerId.toString());
			oos = new ObjectOutputStream(fos);
			oos.writeObject(containerData);
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public byte[] get(Long containerId) {
		byte[] result = null;

		try {
			fis = new FileInputStream(containerDirectory
					+ containerId.toString());
			ois = new ObjectInputStream(fis);
			result = ((byte[]) ois.readObject());
			ois.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}

		return result;
	}
}
