/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;

import org.hibernate.FlushMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.query.named.NamedQueryMemento;
import org.hibernate.query.spi.QueryImplementor;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleIdEntityLoader;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

import jakarta.persistence.Parameter;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * Implementation of SingleIdEntityLoader for cases where the application has
 * provided the select load query
 */
public class ReactiveSingleIdEntityLoaderProvidedQueryImpl<T> implements ReactiveSingleIdEntityLoader<T> {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private static final CompletionStage<Object[]> EMPTY_ARRAY_STAGE = completedFuture( ArrayHelper.EMPTY_OBJECT_ARRAY );

	private final EntityMappingType entityDescriptor;
	private final NamedQueryMemento namedQueryMemento;

	public ReactiveSingleIdEntityLoaderProvidedQueryImpl(EntityMappingType entityDescriptor, NamedQueryMemento namedQueryMemento) {
		this.entityDescriptor = entityDescriptor;
		this.namedQueryMemento = namedQueryMemento;
	}

	@Override
	public EntityMappingType getLoadable() {
		return entityDescriptor;
	}

	@Override
	public CompletionStage<T> load(Object pkValue, LockOptions lockOptions, Boolean readOnly, SharedSessionContractImplementor session) {
		// noinspection unchecked
		final QueryImplementor<T> query = namedQueryMemento
				.toQuery( session, (Class<T>) entityDescriptor.getMappedJavaType().getJavaTypeClass() );

		//noinspection unchecked
		query.setParameter( (Parameter<Object>) query.getParameters().iterator().next(), pkValue );
		query.setHibernateFlushMode( FlushMode.MANUAL );

		return completedFuture( query.uniqueResult() );
	}

	@Override
	public CompletionStage<T> load(
			Object pkValue,
			Object entityInstance,
			LockOptions lockOptions,
			Boolean readOnly,
			SharedSessionContractImplementor session) {
		if ( entityInstance != null ) {
			throw LOG.notYetImplemented();
		}
		return load( pkValue, lockOptions, readOnly, session );
	}

	@Override
	public CompletionStage<Object[]> reactiveLoadDatabaseSnapshot(Object id, SharedSessionContractImplementor session) {
		return EMPTY_ARRAY_STAGE;
	}
}
