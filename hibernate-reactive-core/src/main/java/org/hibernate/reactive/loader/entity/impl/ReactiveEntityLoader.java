/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.entity.impl;

import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.loader.entity.EntityJoinWalker;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.type.Type;

/**
 * A reactific {@link org.hibernate.loader.entity.EntityLoader}.
 *
 * This one doesn't support the JPA {@link javax.persistence.EntityGraph},
 * so for fetch plan support see {@link ReactivePlanEntityLoader}.
 *
 * @see org.hibernate.loader.entity.EntityLoader
 */
public class ReactiveEntityLoader extends ReactiveAbstractEntityLoader {

	private final int[][] compositeKeyManyToOneTargetIndices;

	public ReactiveEntityLoader(
			OuterJoinLoadable persister,
			SessionFactoryImplementor factory,
			LockMode lockMode,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this( persister, 1, lockMode, factory, loadQueryInfluencers );
	}

	public ReactiveEntityLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockMode lockMode,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(
				persister,
				persister.getIdentifierType(),
				factory,
				loadQueryInfluencers,
				new EntityJoinWalker(
						persister,
						persister.getIdentifierColumnNames(),
						batchSize,
						lockMode,
						factory,
						loadQueryInfluencers
				) );

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf( "Static select for entity %s [%s]: %s", entityName, lockMode, getSQLString() );
		}
	}

	public ReactiveEntityLoader(
			OuterJoinLoadable persister,
			int batchSize,
			LockOptions lockOptions,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers) throws MappingException {
		this(
				persister,
				persister.getIdentifierType(),
				factory,
				loadQueryInfluencers,
				new EntityJoinWalker(
						persister,
						persister.getIdentifierColumnNames(),
						batchSize,
						lockOptions,
						factory,
						loadQueryInfluencers
				) );

		if ( LOG.isDebugEnabled() ) {
			LOG.debugf( "Static select for entity %s [%s:%s]: %s",
					entityName,
					lockOptions.getLockMode(),
					lockOptions.getTimeOut(),
					getSQLString() );
		}
	}

	private ReactiveEntityLoader(
			OuterJoinLoadable persister,
			Type uniqueKeyType,
			SessionFactoryImplementor factory,
			LoadQueryInfluencers loadQueryInfluencers,
			EntityJoinWalker walker) throws MappingException {
		super( persister, uniqueKeyType, factory, loadQueryInfluencers );

		initFromWalker( walker );
		compositeKeyManyToOneTargetIndices = walker.getCompositeKeyManyToOneTargetIndices();
		postInstantiate();
	}

	@Override
	public int[][] getCompositeKeyManyToOneTargetIndices() {
		return compositeKeyManyToOneTargetIndices;
	}
}
