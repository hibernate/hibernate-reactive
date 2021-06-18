/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.loader.collection.BasicCollectionJoinWalker;
import org.hibernate.persister.collection.QueryableCollection;

/**
 * A reactive {@link org.hibernate.loader.collection.BasicCollectionLoader}
 */
public class ReactiveBasicCollectionLoader extends ReactiveCollectionLoader {
	public ReactiveBasicCollectionLoader(
			QueryableCollection collectionPersister,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		this( collectionPersister, 1, factory, loadQueryInfluencers );
	}

	public ReactiveBasicCollectionLoader(
			QueryableCollection collectionPersister,
			int batchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(collectionPersister, batchSize, null, factory, loadQueryInfluencers);
	}

	public ReactiveBasicCollectionLoader(
			QueryableCollection collectionPersister,
			int batchSize,
			String subquery,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		super(collectionPersister, factory, loadQueryInfluencers);

		initFromWalker( new BasicCollectionJoinWalker(
				collectionPersister,
				batchSize,
				subquery,
				factory,
				loadQueryInfluencers
		) );

		postInstantiate();
		if ( LOG.isDebugEnabled() ) {
			LOG.debugf(
					"Static select for one-to-many %s: %s",
					collectionPersister.getRole(),
					getSQLString()
			);
		}
	}
}
