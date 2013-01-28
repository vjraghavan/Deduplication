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
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;

public class ContainerMetadataStore {

	/* An entity class. */
	@Entity
	static class ContainerMetadata {

		@PrimaryKey
		String key;
		Metadata metadata;

		public ContainerMetadata(String key, Metadata metadata) {
			this.key = key;
			this.metadata = metadata;
		}

		private ContainerMetadata() {
		} // For deserialization
	}

	@Persistent
	static class Metadata {

		List<SegmentMetadata> metadataList;

		public Metadata(List<SegmentMetadata> metadataList) {
			this.metadataList = metadataList;
		}

		public Metadata() {
		}
	}

	@Persistent
	public static class SegmentMetadata {

		String hash;
		long offset;
		int length;

		public SegmentMetadata(String hash, long offset, int length) {
			this.hash = hash;
			this.offset = offset;
			this.length = length;
		}

		public SegmentMetadata() {
		}
	}

	/* The data accessor class for the entity model. */
	static class MetadataAccessor {

		/* Person accessors */
		PrimaryIndex<String, ContainerMetadata> metadataByContainerId;

		/* Opens all primary and secondary indices. */
		public MetadataAccessor(EntityStore metadataStore)
				throws DatabaseException {

			metadataByContainerId = metadataStore.getPrimaryIndex(String.class,
					ContainerMetadata.class);
		}
	}

	private Environment env;
	private EntityStore metadataStore;
	private MetadataAccessor dao;

	public ContainerMetadataStore(File envHome) throws DatabaseException {

		/* Open a transactional Berkeley DB engine environment. */
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		env = new Environment(envHome, envConfig);

		/* Open a transactional entity store. */
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		storeConfig.setTransactional(true);
		metadataStore = new EntityStore(env, "MetadataIndex", storeConfig);

		/* Initialize the data access object. */
		dao = new MetadataAccessor(metadataStore);
	}

	public void put(String containerId, List<SegmentMetadata> metadataContainer) {
		dao.metadataByContainerId.put(new ContainerMetadata(containerId,
				new Metadata(metadataContainer)));
	}

	public List<SegmentMetadata> get(String containerId) {
		return dao.metadataByContainerId.get(containerId).metadata.metadataList;
	}

	private void close() throws DatabaseException {

		metadataStore.close();
		env.close();
	}

	public static void main(String[] args) throws DatabaseException {

		ContainerMetadataStore putGet = new ContainerMetadataStore(new File(
				"/home/vijay/containerDB"));
		putGet.close();
	}
}