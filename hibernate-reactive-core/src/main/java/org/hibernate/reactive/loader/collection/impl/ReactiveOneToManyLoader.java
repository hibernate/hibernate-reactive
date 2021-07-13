/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.collection.impl;

import java.lang.invoke.MethodHandles;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.loader.collection.OneToManyJoinWalker;
import org.hibernate.loader.collection.OneToManyLoader;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

/**
 * A reactive {@link OneToManyLoader}
 */
public class ReactiveOneToManyLoader extends ReactiveCollectionLoader {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveOneToManyLoader(
			QueryableCollection oneToManyPersister,
			SessionFactoryImplementor session,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(oneToManyPersister, 1, session, loadQueryInfluencers);
	}

	public ReactiveOneToManyLoader(
			QueryableCollection oneToManyPersister,
			int batchSize,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(oneToManyPersister, batchSize, null, factory, loadQueryInfluencers);
	}

	public ReactiveOneToManyLoader(
			QueryableCollection oneToManyPersister,
			int batchSize,
			String subquery,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		super(oneToManyPersister, factory, loadQueryInfluencers);

		initFromWalker( new OneToManyJoinWalker(
				oneToManyPersister,
				batchSize,
				subquery,
				factory,
				loadQueryInfluencers
		) );

		postInstantiate();
		if ( LOG.isDebugEnabled() ) {
			LOG.debugf(
					"Static select for one-to-many %s: %s",
					oneToManyPersister.getRole(),
					getSQLString()
			);
		}
	}
}
