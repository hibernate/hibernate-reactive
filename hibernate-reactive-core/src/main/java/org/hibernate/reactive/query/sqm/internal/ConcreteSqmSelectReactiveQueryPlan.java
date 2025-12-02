/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.internal;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.ScrollMode;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.internal.util.MutableObject;
import org.hibernate.query.Query;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.ScrollableResultsImplementor;
import org.hibernate.query.sqm.internal.CacheableSqmInterpretation;
import org.hibernate.query.sqm.internal.ConcreteSqmSelectQueryPlan;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;
import org.hibernate.reactive.query.sqm.spi.ReactiveSelectQueryPlan;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.reactive.sql.results.spi.ReactiveResultsConsumer;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.exec.spi.JdbcParametersList;
import org.hibernate.sql.results.internal.TupleMetadata;
import org.hibernate.sql.results.spi.RowTransformer;

import static java.util.Collections.emptyList;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * Standard Hibernate implementation of SelectQueryPlan for SQM-backed
 * {@link Query} implementations, which means
 * HQL/JPQL or {@link jakarta.persistence.criteria.CriteriaQuery}
 *
 * @see org.hibernate.query.sqm.internal.ConcreteSqmSelectQueryPlan
 */
public class ConcreteSqmSelectReactiveQueryPlan<R> extends ConcreteSqmSelectQueryPlan<R>
		implements ReactiveSelectQueryPlan<R> {

	private final SqmInterpreter<Object, ReactiveResultsConsumer<Object, R>> executeQueryInterpreter;
	private final SqmInterpreter<List<R>, Void> listInterpreter;
	private final RowTransformer<R> rowTransformer;

	private final SqmSelectStatement<?> sqm;
	private final DomainParameterXref domainParameterXref;

	private volatile CacheableSqmInterpretation<SelectStatement, JdbcOperationQuerySelect> cacheableSqmInterpretation;

	public ConcreteSqmSelectReactiveQueryPlan(
			SqmSelectStatement<?> sqm,
			String hql,
			DomainParameterXref domainParameterXref,
			Class<R> resultType,
			TupleMetadata tupleMetadata,
			QueryOptions queryOptions) {
		super( sqm, hql, domainParameterXref, resultType, tupleMetadata, queryOptions );
		this.sqm = sqm;
		this.domainParameterXref = domainParameterXref;
		this.rowTransformer = determineRowTransformer( sqm, resultType, tupleMetadata, queryOptions );
		this.listInterpreter = (unused, executionContext, sqmInterpretation, jdbcParameterBindings) ->
				listInterpreter( hql, domainParameterXref, executionContext, sqmInterpretation, jdbcParameterBindings, rowTransformer );
		this.executeQueryInterpreter = (resultsConsumer, executionContext, sqmInterpretation, jdbcParameterBindings) ->
				executeQueryInterpreter( hql, domainParameterXref, executionContext, sqmInterpretation, jdbcParameterBindings, rowTransformer, resultsConsumer );
	}

	private static <R> CompletionStage<List<R>> listInterpreter(
			String hql,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext executionContext,
			CacheableSqmInterpretation<SelectStatement, JdbcOperationQuerySelect> sqmInterpretation,
			JdbcParameterBindings jdbcParameterBindings,
			RowTransformer<R> rowTransformer) {
		final ReactiveSharedSessionContractImplementor session = (ReactiveSharedSessionContractImplementor) executionContext.getSession();
		final JdbcOperationQuerySelect jdbcSelect = sqmInterpretation.jdbcOperation();

		return CompletionStages
				.supplyStage( () -> {
					final var subSelectFetchKeyHandler = SubselectFetch.createRegistrationHandler(
							session.getPersistenceContext()
									.getBatchFetchQueue(),
							sqmInterpretation.statement(),
							JdbcParametersList.empty(),
							jdbcParameterBindings
					);
					return session
							.reactiveAutoFlushIfRequired( jdbcSelect.getAffectedTableNames() )
							.thenCompose( required -> {
								final Expression fetchExpression = sqmInterpretation.statement()
										.getQueryPart()
										.getFetchClauseExpression();
								final int resultCountEstimate = fetchExpression != null
										? interpretIntExpression( fetchExpression, jdbcParameterBindings )
										: -1;
								return StandardReactiveSelectExecutor.INSTANCE.list(
										jdbcSelect,
										jdbcParameterBindings,
										ConcreteSqmSelectQueryPlan.listInterpreterExecutionContext(
												hql,
												executionContext,
												jdbcSelect,
												subSelectFetchKeyHandler
										),
										rowTransformer,
										(Class<R>) executionContext.getResultType(),
										ReactiveListResultsConsumer.UniqueSemantic.ALLOW,
										resultCountEstimate
								);
							} );
				} )
				.whenComplete( (rs, t) -> domainParameterXref.clearExpansions() );
	}

	private static <R> CompletionStage<Object> executeQueryInterpreter(
			String hql,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext executionContext,
			CacheableSqmInterpretation<SelectStatement, JdbcOperationQuerySelect> sqmInterpretation,
			JdbcParameterBindings jdbcParameterBindings,
			RowTransformer<R> rowTransformer,
			ReactiveResultsConsumer<Object, R> resultsConsumer) {
		final ReactiveSharedSessionContractImplementor session = (ReactiveSharedSessionContractImplementor) executionContext.getSession();
		final JdbcOperationQuerySelect jdbcSelect = sqmInterpretation.jdbcOperation();
		return CompletionStages
				.supplyStage( () -> {
					final var subSelectFetchKeyHandler = SubselectFetch.createRegistrationHandler(
							session.getPersistenceContext()
									.getBatchFetchQueue(),
							sqmInterpretation.statement(),
							JdbcParametersList.empty(),
							jdbcParameterBindings
					);
					return session
							.reactiveAutoFlushIfRequired( jdbcSelect.getAffectedTableNames() )
							.thenCompose( required -> StandardReactiveSelectExecutor.INSTANCE
									.executeQuery(
											jdbcSelect,
											jdbcParameterBindings,
											ConcreteSqmSelectQueryPlan.listInterpreterExecutionContext(
													hql,
													executionContext,
													jdbcSelect,
													subSelectFetchKeyHandler
											),
											rowTransformer,
											null,
											resultCountEstimate( sqmInterpretation, jdbcParameterBindings ),
											resultsConsumer
									)
							);
				})
				.whenComplete( (rs, t) -> domainParameterXref.clearExpansions() );
	}

	private static int resultCountEstimate(
			CacheableSqmInterpretation<SelectStatement,JdbcOperationQuerySelect> sqmInterpretation,
			JdbcParameterBindings jdbcParameterBindings) {
		final Expression fetchExpression = sqmInterpretation.statement().getQueryPart()
				.getFetchClauseExpression();
		return fetchExpression != null
				? interpretIntExpression( fetchExpression, jdbcParameterBindings )
				: -1;
	}

	@Override
	public ScrollableResultsImplementor<R> performScroll(ScrollMode scrollMode, DomainQueryExecutionContext executionContext) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<List<R>> reactivePerformList(DomainQueryExecutionContext executionContext) {
		return executionContext.getQueryOptions().getEffectiveLimit().getMaxRowsJpa() == 0
				? completedFuture( emptyList() )
				: withCacheableSqmInterpretation( executionContext, null, listInterpreter );
	}

	@Override
	public <T> CompletionStage<T> reactiveExecuteQuery(
			DomainQueryExecutionContext executionContext,
			ReactiveResultsConsumer<T, R> resultsConsumer) {
		return withCacheableSqmInterpretation(
				executionContext,
				resultsConsumer,
				(SqmInterpreter<T, ReactiveResultsConsumer<T, R>>) (SqmInterpreter) executeQueryInterpreter
		);
	}

	private <T, X> CompletionStage<T> withCacheableSqmInterpretation(DomainQueryExecutionContext executionContext, X context, SqmInterpreter<T, X> interpreter) {
		// NOTE : VERY IMPORTANT - intentional double-lock checking
		//		The other option would be to leverage `java.util.concurrent.locks.ReadWriteLock`
		//		to protect access.  However, synchronized is much simpler here.  We will verify
		// 		during throughput testing whether this is an issue and consider changes then

		CacheableSqmInterpretation<SelectStatement, JdbcOperationQuerySelect> localCopy = cacheableSqmInterpretation;
		JdbcParameterBindings jdbcParameterBindings = null;

		if ( localCopy == null ) {
			synchronized ( this ) {
				localCopy = cacheableSqmInterpretation;
				if ( localCopy == null ) {
					final MutableObject<JdbcParameterBindings> mutableValue = new MutableObject<>();
					localCopy = buildInterpretation( sqm, domainParameterXref, executionContext, mutableValue );
					jdbcParameterBindings = mutableValue.get();
					cacheableSqmInterpretation = localCopy;
				}
			}
		}
		else {
			// If the translation depends on parameter bindings, or it isn't compatible with the current query options,
			// we have to rebuild the JdbcSelect, which is still better than having to translate from SQM to SQL AST again
			if ( localCopy.jdbcOperation().dependsOnParameterBindings() ) {
				jdbcParameterBindings = createJdbcParameterBindings( localCopy, executionContext );
			}
			// If the translation depends on the limit or lock options, we have to rebuild the JdbcSelect
			// We could avoid this by putting the lock options into the cache key
			if ( !localCopy.jdbcOperation().isCompatibleWith( jdbcParameterBindings, executionContext.getQueryOptions() ) ) {
				final MutableObject<JdbcParameterBindings> mutableValue = new MutableObject<>();
				localCopy = buildInterpretation( sqm, domainParameterXref, executionContext, mutableValue );
				jdbcParameterBindings = mutableValue.get();
				cacheableSqmInterpretation = localCopy;
			}
		}

		if ( jdbcParameterBindings == null ) {
			jdbcParameterBindings = createJdbcParameterBindings( localCopy, executionContext );
		}

		return interpreter.interpret( context, executionContext, localCopy, jdbcParameterBindings );
	}

	private interface SqmInterpreter<T, X> {
		CompletionStage<T> interpret(
				X context,
				DomainQueryExecutionContext executionContext,
				CacheableSqmInterpretation<SelectStatement, JdbcOperationQuerySelect> sqmInterpretation,
				JdbcParameterBindings jdbcParameterBindings);
	}

}
