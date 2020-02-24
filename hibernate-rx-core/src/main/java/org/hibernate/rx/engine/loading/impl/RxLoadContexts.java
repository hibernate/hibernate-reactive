package org.hibernate.rx.engine.loading.impl;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.loading.internal.CollectionLoadContext;
import org.hibernate.engine.loading.internal.EntityLoadContext;
import org.hibernate.engine.loading.internal.LoadContexts;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.persister.collection.CollectionPersister;

import java.sql.ResultSet;

public class RxLoadContexts extends LoadContexts {
	private final LoadContexts delegate;

	public RxLoadContexts(LoadContexts contexts) {
		super(contexts.getPersistenceContext());
		this.delegate = contexts;
	}

	@Override
	public CollectionLoadContext getCollectionLoadContext(ResultSet resultSet) {
		return getCollectionLoadContext(resultSet);
	}

	//////////////////
	// DELEGATES
	//////////////////

	@Override
	public PersistenceContext getPersistenceContext() {
		return delegate.getPersistenceContext();
	}

	@Override
	public void cleanup(ResultSet resultSet) {
		delegate.cleanup(resultSet);
	}

	@Override
	public void cleanup() {
		delegate.cleanup();
	}

	@Override
	public boolean hasLoadingCollectionEntries() {
		return delegate.hasLoadingCollectionEntries();
	}

	@Override
	public boolean hasRegisteredLoadingCollectionEntries() {
		return delegate.hasRegisteredLoadingCollectionEntries();
	}

	@Override
	public PersistentCollection locateLoadingCollection(CollectionPersister persister, CollectionKey key) {
		return delegate.locateLoadingCollection(persister, key);
	}

	@Override
	public EntityLoadContext getEntityLoadContext(ResultSet resultSet) {
		return delegate.getEntityLoadContext(resultSet);
	}
}
