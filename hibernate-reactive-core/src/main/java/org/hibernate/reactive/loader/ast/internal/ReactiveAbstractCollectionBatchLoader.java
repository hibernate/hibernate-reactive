/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;


import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.CollectionEntry;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.reactive.loader.ast.spi.ReactiveCollectionBatchLoader;
import org.hibernate.sql.results.internal.ResultsHelper;

import static org.hibernate.loader.ast.internal.MultiKeyLoadLogging.MULTI_KEY_LOAD_DEBUG_ENABLED;
import static org.hibernate.loader.ast.internal.MultiKeyLoadLogging.MULTI_KEY_LOAD_LOGGER;

/**
 * @see org.hibernate.loader.ast.internal.AbstractCollectionBatchLoader
 */
public abstract class ReactiveAbstractCollectionBatchLoader implements ReactiveCollectionBatchLoader {
	private final int domainBatchSize;
	private final PluralAttributeMapping attributeMapping;
	private final LoadQueryInfluencers influencers;
	private final SessionFactoryImplementor sessionFactory;

	private final int keyJdbcCount;

	public ReactiveAbstractCollectionBatchLoader(
			int domainBatchSize,
			LoadQueryInfluencers influencers,
			PluralAttributeMapping attributeMapping,
			SessionFactoryImplementor sessionFactory) {
		this.domainBatchSize = domainBatchSize;
		this.attributeMapping = attributeMapping;

		this.keyJdbcCount = attributeMapping.getJdbcTypeCount();
		this.sessionFactory = sessionFactory;
		this.influencers = influencers;
	}

	@Override
	public int getDomainBatchSize() {
		return domainBatchSize;
	}

	@Override
	public PluralAttributeMapping getLoadable() {
		return attributeMapping;
	}

	public LoadQueryInfluencers getInfluencers() {
		return influencers;
	}

	public SessionFactoryImplementor getSessionFactory() {
		return sessionFactory;
	}

	public int getKeyJdbcCount() {
		return keyJdbcCount;
	}

	protected void finishInitializingKey(
			Object key,
			SharedSessionContractImplementor session) {
		if ( key == null ) {
			return;
		}

		if ( MULTI_KEY_LOAD_DEBUG_ENABLED ) {
			MULTI_KEY_LOAD_LOGGER.debugf( "Finishing initializing batch-fetched collection : %s.%s", attributeMapping.getNavigableRole().getFullPath(), key );
		}

		final PersistenceContext persistenceContext = session.getPersistenceContext();
		final CollectionKey collectionKey = new CollectionKey( getLoadable().getCollectionDescriptor(), key );
		final PersistentCollection<?> collection = persistenceContext.getCollection( collectionKey );
		if ( !collection.wasInitialized() ) {
			final CollectionEntry entry = persistenceContext.getCollectionEntry( collection );
			collection.initializeEmptyCollection( entry.getLoadedPersister() );
			ResultsHelper.finalizeCollectionLoading(
					persistenceContext,
					entry.getLoadedPersister(),
					collection,
					collectionKey,
					true
			);
		}

	}
}
