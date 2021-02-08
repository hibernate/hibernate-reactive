/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.collection.impl;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.collection.OneToManyJoinWalker;
import org.hibernate.persister.collection.QueryableCollection;
import org.hibernate.transform.ResultTransformer;

public class ReactiveBasicCollectionLoader extends ReactiveCollectionLoader {
	public ReactiveBasicCollectionLoader(
			QueryableCollection collectionPersister,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) {
		super( collectionPersister, factory, loadQueryInfluencers );
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

		initFromWalker( new OneToManyJoinWalker(
				collectionPersister,
				batchSize,
				subquery,
				factory,
				loadQueryInfluencers
		) );

		postInstantiate();
		if (LOG.isDebugEnabled()) {
			LOG.debugf("Static select for one-to-many %s: %s", collectionPersister.getRole(), getSQLString());
		}
	}

	@Override
	protected Object getResultColumnOrRow(
			Object[] row,
			ResultTransformer transformer,
			ResultSet rs, SharedSessionContractImplementor session) throws SQLException, HibernateException {
		if ( row.length == 1 ) {
			return row[0];
		}
		return row;
	}
}
