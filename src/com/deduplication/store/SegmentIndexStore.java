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

public class SegmentIndexStore {

	/* An entity class. */
	@Entity
	static class SegmentIdentifier {

		@PrimaryKey
		String key;
		String containerId;

		SegmentIdentifier(String key, String containerId) {
			this.key = key;
			this.containerId = containerId;
		}

		private SegmentIdentifier() {
		} // For deserialization
	}

	/* The data accessor class for the entity model. */
	static class SegmentIndexAccessor {

		/* Person accessors */
		PrimaryIndex<String, SegmentIdentifier> containerIdByHash;

		/* Opens all primary and secondary indices. */
		public SegmentIndexAccessor(EntityStore store) throws DatabaseException {

			containerIdByHash = store.getPrimaryIndex(String.class,
					SegmentIdentifier.class);
		}
	}

	private Environment env;
	private EntityStore store;
	private SegmentIndexAccessor dao;

	public SegmentIndexStore(File envHome) throws DatabaseException {

		/* Open a transactional Berkeley DB engine environment. */
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		env = new Environment(envHome, envConfig);

		/* Open a transactional entity store. */
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		storeConfig.setTransactional(true);
		store = new EntityStore(env, "SegmentIndex", storeConfig);

		/* Initialize the data access object. */
		dao = new SegmentIndexAccessor(store);
	}

	private void run() throws DatabaseException {

		//SegmentIdentifier one = new SegmentIdentifier("Bob Smith", new String(
		//		"first"));
		// dao.containerIdByHash.put(one);
		// System.out.println("Key :" + one.key + "Value : " + one.containerId
		// );

		// dao.containerIdByHash.put(new SegmentIdentifier("Mary Smith", new
		// String("two")));

		SegmentIdentifier resultIdentifier = dao.containerIdByHash
				.get("Bob Smith");
		assert resultIdentifier != null;
		System.out.println("Key :" + "Bob Smith, " + "Value : "
				+ resultIdentifier.containerId);
	}

	private void close() throws DatabaseException {

		store.close();
		env.close();
	}

	public static void main(String[] args) throws DatabaseException {

		SegmentIndexStore putGet = new SegmentIndexStore(new File("/home/vijay/testDb"));
		putGet.run();
		putGet.close();
	}
}