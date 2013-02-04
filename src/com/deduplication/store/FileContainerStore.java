package com.deduplication.store;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class FileContainerStore {

	FileInputStream fis;
	ObjectInputStream ois;
	FileOutputStream fos;
	ObjectOutputStream oos;
	String containerDirectory;

	public FileContainerStore(String containerDirectory) {
		this.containerDirectory = containerDirectory;
	}

	public void put(Long containerId, List<Byte> containerData) {

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

	@SuppressWarnings("unchecked")
	public List<Byte> get(Long containerId) {
		List<Byte> result = null;

		try {
			fis = new FileInputStream(containerDirectory
					+ containerId.toString());
			ois = new ObjectInputStream(fis);
			result = ((List<Byte>) ois.readObject());
			ois.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}

		return result;
	}

	public static void main(String[] args) {

		List<Byte> data1 = new ArrayList<Byte>(30);

		for (int i = 0; i < 30; i++)
			data1.add((byte) i);
		System.out.println(data1.size());
		FileContainerStore fs = new FileContainerStore(
				"/home/vijay/Archive/FileContainerStore/");
		fs.put(new Long(1), data1);
		System.out.println("Done serialization");
		List<Byte> result = fs.get(new Long(1));
		System.out.println("Done deserialization");
		System.out.println(result.size());
		for(byte b : result)
			System.out.print(b + " ");
	}

}
