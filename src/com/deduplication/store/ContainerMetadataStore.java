package com.deduplication.store;

/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

import java.io.File;
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
		Long key;
		Metadata metadata;

		public ContainerMetadata(Long key, Metadata metadata) {
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

		public String hash;
		public int length;

		public SegmentMetadata(String hash, int length) {
			this.hash = hash;
			this.length = length;
		}

		public SegmentMetadata() {
		}
	}

	/* The data accessor class for the entity model. */
	static class MetadataAccessor {

		/* Person accessors */
		PrimaryIndex<Long, ContainerMetadata> metadataByContainerId;

		/* Opens all primary and secondary indices. */
		public MetadataAccessor(EntityStore metadataStore)
				throws DatabaseException {

			metadataByContainerId = metadataStore.getPrimaryIndex(Long.class,
					ContainerMetadata.class);
		}
	}

	private Environment env;
	private EntityStore metadataStore;
	private MetadataAccessor dao;

	public ContainerMetadataStore(File envHome, EnvironmentConfig envConfig)
			throws DatabaseException {

		/* Open a transactional Berkeley DB engine environment. */
		env = new Environment(envHome, envConfig);

		/* Open a transactional entity store. */
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		storeConfig.setTransactional(true);
		metadataStore = new EntityStore(env, "MetadataIndex", storeConfig);

		/* Initialize the data access object. */
		dao = new MetadataAccessor(metadataStore);
	}

	public void put(Long containerId, List<SegmentMetadata> metadataContainer) {
		dao.metadataByContainerId.put(new ContainerMetadata(containerId,
				new Metadata(metadataContainer)));
	}

	public List<SegmentMetadata> get(Long containerId) {
		ContainerMetadata containerMetadata = dao.metadataByContainerId
				.get(containerId);
		return (containerMetadata == null ? null
				: containerMetadata.metadata.metadataList);
	}

	public void close() throws DatabaseException {

		metadataStore.close();
		env.close();
	}

	public static void main(String[] args) throws DatabaseException {

		/*
		 * ContainerMetadataStore putGet = new ContainerMetadataStore(new File(
		 * "/home/vijay/containerDB")); putGet.close();
		 */
	}
}