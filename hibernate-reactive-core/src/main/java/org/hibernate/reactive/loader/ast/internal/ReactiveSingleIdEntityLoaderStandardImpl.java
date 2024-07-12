/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.concurrent.CompletionStage;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.internal.LoaderSelectBuilder;
import org.hibernate.loader.ast.internal.SingleIdEntityLoaderStandardImpl;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleIdEntityLoader;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.spi.JdbcParametersList;

/**
 * Standard implementation of {@link ReactiveSingleIdEntityLoader}.
 *
 * @see SingleIdEntityLoaderStandardImpl
 */
public class ReactiveSingleIdEntityLoaderStandardImpl<T> extends SingleIdEntityLoaderStandardImpl<CompletionStage<T>>
		implements ReactiveSingleIdEntityLoader<T> {

	private DatabaseSnapshotExecutor databaseSnapshotExecutor;

	public ReactiveSingleIdEntityLoaderStandardImpl(
			EntityMappingType entityDescriptor,
			LoadQueryInfluencers loadQueryInfluencers) {
		super(
				entityDescriptor,
				loadQueryInfluencers,
				(lockOptions, influencers) -> createLoadPlan( entityDescriptor, lockOptions, influencers, influencers.getSessionFactory() )
		);
	}

	@Override
	public CompletionStage<Object[]> reactiveLoadDatabaseSnapshot(Object id, SharedSessionContractImplementor session) {
		if ( databaseSnapshotExecutor == null ) {
			databaseSnapshotExecutor = new DatabaseSnapshotExecutor( getLoadable(), sessionFactory );
		}

		return databaseSnapshotExecutor.loadDatabaseSnapshot( id, session );
	}

	@Override
	public CompletionStage<T> load(
			Object key,
			LockOptions lockOptions,
			Boolean readOnly,
			SharedSessionContractImplementor session) {
		final ReactiveSingleIdLoadPlan<T> loadPlan = (ReactiveSingleIdLoadPlan<T>) resolveLoadPlan(
				lockOptions,
				session.getLoadQueryInfluencers(),
				session.getFactory()
		);
		return loadPlan.load( key, readOnly, true, session );
	}

	@Override
	public CompletionStage<T> load(
			Object key,
			Object entityInstance,
			LockOptions lockOptions,
			Boolean readOnly,
			SharedSessionContractImplementor session) {
		final ReactiveSingleIdLoadPlan<T> loadPlan = (ReactiveSingleIdLoadPlan<T>) resolveLoadPlan( lockOptions, session.getLoadQueryInfluencers(), session.getFactory() );
		return loadPlan.load( key, entityInstance, readOnly, false, session );
	}

	private static <T> ReactiveSingleIdLoadPlan<T> createLoadPlan(
			EntityMappingType loadable,
			LockOptions lockOptions,
			LoadQueryInfluencers queryInfluencers,
			SessionFactoryImplementor sessionFactory) {
		final JdbcParametersList.Builder jdbcParametersListBuilder = JdbcParametersList.newBuilder();
		final SelectStatement sqlAst = LoaderSelectBuilder.createSelect(
				loadable,
				// null here means to select everything
				null,
				loadable.getIdentifierMapping(),
				null,
				1,
				queryInfluencers,
				lockOptions,
				jdbcParametersListBuilder::add,
				sessionFactory
		);
		final JdbcParametersList jdbcParameters = jdbcParametersListBuilder.build();

		return new ReactiveSingleIdLoadPlan<>(
				loadable,
				loadable.getIdentifierMapping(),
				sqlAst,
				jdbcParameters,
				lockOptions,
				sessionFactory
		);
	}
}
