/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.internal.SingleIdLoadPlan;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.query.internal.SimpleQueryOptions;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.CallbackImpl;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.Callback;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcParametersList;
import org.hibernate.sql.results.graph.entity.LoadingEntityEntry;

public class ReactiveSingleIdLoadPlan<T> extends SingleIdLoadPlan<CompletionStage<T>> {

	public ReactiveSingleIdLoadPlan(
			Loadable persister,
			ModelPart restrictivePart,
			SelectStatement sqlAst,
			JdbcParametersList jdbcParameters,
			LockOptions lockOptions,
			SessionFactoryImplementor sessionFactory) {
		super( persister, restrictivePart, sqlAst, jdbcParameters, lockOptions, sessionFactory );
	}

	@Override
	public CompletionStage<T> load(Object restrictedValue, Object entityInstance, Boolean readOnly, Boolean singleResultExpected, SharedSessionContractImplementor session) {
		final int jdbcTypeCount = getRestrictivePart().getJdbcTypeCount();
		assert getJdbcParameters().size() % jdbcTypeCount == 0;
		final JdbcParameterBindings jdbcParameterBindings = new JdbcParameterBindingsImpl( jdbcTypeCount );

		int offset = 0;
		while ( offset < getJdbcParameters().size() ) {
			offset += jdbcParameterBindings.registerParametersForEachJdbcValue(
					restrictedValue,
					offset,
					getRestrictivePart(),
					getJdbcParameters(),
					session
			);
		}
		assert offset == getJdbcParameters().size();
		final QueryOptions queryOptions = new SimpleQueryOptions( getLockOptions(), readOnly );
		final Callback callback = new CallbackImpl();
		ExecutionContext executionContext = executionContext( restrictedValue, entityInstance, session, queryOptions, callback );
		// FIXME: Should we get this from jdbcServices.getSelectExecutor()?
		return StandardReactiveSelectExecutor.INSTANCE
				.list( getJdbcSelect(), jdbcParameterBindings, executionContext, getRowTransformer(), resultConsumer( singleResultExpected ) )
				.thenApply( this::extractEntity )
				.thenApply( entity -> {
					invokeAfterLoadActions( callback, session, entity );
					return (T) entity;
				} );
	}

	private <T> void invokeAfterLoadActions(Callback callback, SharedSessionContractImplementor session, T entity) {
		if ( entity != null && getLoadable() != null) {
			callback.invokeAfterLoadActions( session, entity, (Loadable) getLoadable() );
		}
	}

	private Object extractEntity(List<?> list) {
		return list.isEmpty() ? null : list.get( 0 );
	}

	private static ExecutionContext executionContext(
			Object restrictedValue,
			Object entityInstance,
			SharedSessionContractImplementor session,
			QueryOptions queryOptions,
			Callback callback) {
		return new ExecutionContext() {
			@Override
			public boolean isScrollResult() {
				return ExecutionContext.super.isScrollResult();
			}

			@Override
			public SharedSessionContractImplementor getSession() {
				return session;
			}

			@Override
			public Object getEntityInstance() {
				return entityInstance;
			}

			@Override
			public Object getEntityId() {
				return restrictedValue;
			}

			@Override
			public void registerLoadingEntityEntry(EntityKey entityKey, LoadingEntityEntry entry) {
				ExecutionContext.super.registerLoadingEntityEntry( entityKey, entry );
			}

			@Override
			public void afterStatement(LogicalConnectionImplementor logicalConnection) {
				ExecutionContext.super.afterStatement( logicalConnection );
			}

			@Override
			public boolean hasQueryExecutionToBeAddedToStatistics() {
				return ExecutionContext.super.hasQueryExecutionToBeAddedToStatistics();
			}

			@Override
			public QueryOptions getQueryOptions() {
				return queryOptions;
			}

			@Override
			public LoadQueryInfluencers getLoadQueryInfluencers() {
				return null;
			}

			@Override
			public String getQueryIdentifier(String sql) {
				return sql;
			}

			@Override
			public CollectionKey getCollectionKey() {
				return ExecutionContext.super.getCollectionKey();
			}

			@Override
			public QueryParameterBindings getQueryParameterBindings() {
				return QueryParameterBindings.NO_PARAM_BINDINGS;
			}

			@Override
			public Callback getCallback() {
				return callback;
			}
		};
	}

	private static ReactiveListResultsConsumer.UniqueSemantic resultConsumer(Boolean singleResultExpected) {
		return singleResultExpected
				? ReactiveListResultsConsumer.UniqueSemantic.ASSERT
				: ReactiveListResultsConsumer.UniqueSemantic.FILTER;
	}
}
