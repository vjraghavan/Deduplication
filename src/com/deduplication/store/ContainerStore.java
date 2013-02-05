package com.deduplication.store;
/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

import java.io.File;

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
		Long key;
		byte[] byteContent;

		public Container(Long key, byte[] byteContent) {
			this.key = key;
			this.byteContent = byteContent;
		}

		private Container() {
		} // For deserialization
	}

	/* The data accessor class for the entity model. */
	static class ContainerAccessor {

		/* Person accessors */
		PrimaryIndex<Long, Container> containerByContainerId;

		/* Opens all primary and secondary indices. */
		public ContainerAccessor(EntityStore containerStore)
				throws DatabaseException {

			containerByContainerId = containerStore.getPrimaryIndex(
					Long.class, Container.class);
		}
	}

	private Environment env;
	private EntityStore containerStore;
	private ContainerAccessor containerStoreDao;

	public ContainerStore(File envHome, EnvironmentConfig envConfig) throws DatabaseException {

		/* Open a transactional Berkeley DB engine environment. */
		//EnvironmentConfig envConfig = new EnvironmentConfig();
		//envConfig.setAllowCreate(true);
		//envConfig.setTransactional(true);
		env = new Environment(envHome, envConfig);

		/* Open a transactional entity store. */
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		storeConfig.setTransactional(true);
		containerStore = new EntityStore(env, "ContainerData", storeConfig);

		/* Initialize the data access object. */
		containerStoreDao = new ContainerAccessor(containerStore);
	}

	public void put(Long containerId, byte[] byteContent ){
		
		containerStoreDao.containerByContainerId.put(new Container(containerId, byteContent));
	}
	
	public byte[] get(Long containerId){
		
		Container result = containerStoreDao.containerByContainerId.get(containerId);
		return (result == null ? null : result.byteContent);
	}
	
	public void close() throws DatabaseException {

		containerStore.close();
		env.close();
	}

	public static void main(String[] args) throws DatabaseException {

	/*	ContainerStore putGet = new ContainerStore(new File(
				"/home/vijay/containerDb"));
		putGet.close();*/
	}
}