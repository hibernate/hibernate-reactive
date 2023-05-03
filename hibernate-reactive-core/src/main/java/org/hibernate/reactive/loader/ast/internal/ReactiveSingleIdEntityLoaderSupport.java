/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleIdEntityLoader;

/**
 * @see org.hibernate.loader.ast.internal.SingleIdEntityLoaderSupport
 */
public abstract class ReactiveSingleIdEntityLoaderSupport<T> implements ReactiveSingleIdEntityLoader<T> {
	private final EntityMappingType entityDescriptor;
	protected final SessionFactoryImplementor sessionFactory;

	private DatabaseSnapshotExecutor databaseSnapshotExecutor;

	public ReactiveSingleIdEntityLoaderSupport(EntityMappingType entityDescriptor, SessionFactoryImplementor sessionFactory) {
		this.entityDescriptor = entityDescriptor;
		this.sessionFactory = sessionFactory;
	}

	@Override
	public EntityMappingType getLoadable() {
		return entityDescriptor;
	}

	@Override
	public CompletionStage<Object[]> reactiveLoadDatabaseSnapshot(Object id, SharedSessionContractImplementor session) {
		if ( databaseSnapshotExecutor == null ) {
			databaseSnapshotExecutor = new DatabaseSnapshotExecutor( entityDescriptor, sessionFactory );
		}

		return databaseSnapshotExecutor.loadDatabaseSnapshot( id, session );
	}
}
