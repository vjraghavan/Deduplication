package com.deduplication.store;
/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

public class ContainerStore {

	/* An entity class. */
	@Entity
	static class Container {

		@PrimaryKey
		String key;
		List<Byte> byteContentList;

		public Container(String key, List<Byte> byteContentList) {
			this.key = key;
			this.byteContentList = byteContentList;
		}

		private Container() {
		} // For deserialization
	}

	/* The data accessor class for the entity model. */
	static class ContainerAccessor {

		/* Person accessors */
		PrimaryIndex<String, Container> containerByContainerId;

		/* Opens all primary and secondary indices. */
		public ContainerAccessor(EntityStore containerStore)
				throws DatabaseException {

			containerByContainerId = containerStore.getPrimaryIndex(
					String.class, Container.class);
		}
	}

	private Environment env;
	private EntityStore containerStore;
	private ContainerAccessor containerStoreDao;

	public ContainerStore(File envHome) throws DatabaseException {

		/* Open a transactional Berkeley DB engine environment. */
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		env = new Environment(envHome, envConfig);

		/* Open a transactional entity store. */
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		storeConfig.setTransactional(true);
		containerStore = new EntityStore(env, "ContainerData", storeConfig);

		/* Initialize the data access object. */
		containerStoreDao = new ContainerAccessor(containerStore);
	}

	private void run() throws DatabaseException {

		List<Byte> byteDataList = new ArrayList<Byte>();
		byteDataList.add(new Byte((byte) 1));
		byteDataList.add(new Byte((byte) 2));
		Container container = new Container("key", byteDataList);
		containerStoreDao.containerByContainerId.put(container);

		Container resultContainer = containerStoreDao.containerByContainerId
				.get("key");
		System.out.println("Key : " + resultContainer.key);
		System.out.println("Value");
		List<Byte> byteResultList = resultContainer.byteContentList;
		Iterator<Byte> iter = byteResultList.iterator();
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
	}

	private void close() throws DatabaseException {

		containerStore.close();
		env.close();
	}

	public static void main(String[] args) throws DatabaseException {

		ContainerStore putGet = new ContainerStore(new File(
				"/home/vijay/containerDb"));
		putGet.run();
		putGet.close();
	}
}