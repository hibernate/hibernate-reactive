/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.internal.SingleIdLoadPlan;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.persister.entity.Loadable;
import org.hibernate.query.internal.SimpleQueryOptions;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.reactive.sql.exec.internal.ReactiveSelectExecutorStandardImpl;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.CallbackImpl;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.Callback;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

public class ReactiveSingleIdLoadPlan<T> extends SingleIdLoadPlan<CompletionStage<T>> {

	public ReactiveSingleIdLoadPlan(
			Loadable persister,
			ModelPart restrictivePart,
			SelectStatement sqlAst,
			List<JdbcParameter> jdbcParameters,
			LockOptions lockOptions,
			SessionFactoryImplementor sessionFactory) {
		super( persister, restrictivePart, sqlAst, jdbcParameters, lockOptions, sessionFactory );
	}

	@Override
	public CompletionStage<T> load(Object restrictedValue, Object entityInstance, Boolean readOnly, Boolean singleResultExpected, SharedSessionContractImplementor session) {
		final int jdbcTypeCount = getRestrictivePart().getJdbcTypeCount();
		assert getJdbcParameters().size() % jdbcTypeCount == 0;
		final JdbcParameterBindings jdbcParameterBindings = new JdbcParameterBindingsImpl( jdbcTypeCount );
		getJdbcSelect().bindFilterJdbcParameters( jdbcParameterBindings );

		int offset = 0;
		while ( offset < getJdbcParameters().size() ) {
			offset += jdbcParameterBindings.registerParametersForEachJdbcValue(
					restrictedValue,
					Clause.WHERE,
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
		return new ReactiveSelectExecutorStandardImpl()
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
		if ( list.isEmpty() ) {
			return null;
		}

		return list.get( 0 );
	}

	private static ExecutionContext executionContext(
			Object restrictedValue,
			Object entityInstance,
			SharedSessionContractImplementor session,
			QueryOptions queryOptions,
			Callback callback) {
		return new ExecutionContext() {
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
			public QueryOptions getQueryOptions() {
				return queryOptions;
			}

			@Override
			public String getQueryIdentifier(String sql) {
				return sql;
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
