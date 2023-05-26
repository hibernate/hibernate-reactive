/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm.iternal;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.hibernate.ScrollMode;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.metamodel.mapping.MappingModelExpressible;
import org.hibernate.query.Query;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryParameterImplementor;
import org.hibernate.query.spi.ScrollableResultsImplementor;
import org.hibernate.query.sqm.internal.ConcreteSqmSelectQueryPlan;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.internal.SqmUtil;
import org.hibernate.query.sqm.spi.SqmParameterMappingModelResolutionAccess;
import org.hibernate.query.sqm.sql.SqmTranslation;
import org.hibernate.query.sqm.sql.SqmTranslator;
import org.hibernate.query.sqm.sql.SqmTranslatorFactory;
import org.hibernate.query.sqm.tree.expression.SqmParameter;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;
import org.hibernate.reactive.query.sqm.spi.ReactiveSelectQueryPlan;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.spi.FromClauseAccess;
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

	private final SqmInterpreter<List<R>, Void> listInterpreter;
	private final RowTransformer<R> rowTransformer;

	private final SqmSelectStatement<?> sqm;
	private final DomainParameterXref domainParameterXref;

	private volatile CacheableSqmInterpretation cacheableSqmInterpretation;

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
	}

	private static <R> CompletionStage<List<R>> listInterpreter(
			String hql,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext executionContext,
			CacheableSqmInterpretation sqmInterpretation,
			JdbcParameterBindings jdbcParameterBindings,
			RowTransformer<R> rowTransformer) {
		final ReactiveSharedSessionContractImplementor session = (ReactiveSharedSessionContractImplementor) executionContext.getSession();
		final JdbcOperationQuerySelect jdbcSelect = sqmInterpretation.getJdbcSelect();
		// I'm using a supplier so that the whenComplete at the end will catch any errors, like a finally-block
		Supplier<SubselectFetch.RegistrationHandler> fetchHandlerSupplier = () -> SubselectFetch
				.createRegistrationHandler( session.getPersistenceContext().getBatchFetchQueue(), sqmInterpretation.selectStatement, JdbcParametersList.empty(), jdbcParameterBindings );
		return completedFuture( fetchHandlerSupplier )
				.thenApply( Supplier::get )
				.thenCompose( subSelectFetchKeyHandler ->  session
							.reactiveAutoFlushIfRequired( jdbcSelect.getAffectedTableNames() )
							.thenCompose( required -> StandardReactiveSelectExecutor.INSTANCE
									.list( jdbcSelect,
										   jdbcParameterBindings,
										   ConcreteSqmSelectQueryPlan.listInterpreterExecutionContext( hql, executionContext, jdbcSelect, subSelectFetchKeyHandler ),
										   rowTransformer,
										   ReactiveListResultsConsumer.UniqueSemantic.ALLOW
									)
							)
				)
				.whenComplete( (rs, t) -> domainParameterXref.clearExpansions() );
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

	private <T, X> CompletionStage<T> withCacheableSqmInterpretation(DomainQueryExecutionContext executionContext, X context, SqmInterpreter<T, X> interpreter) {
		// NOTE : VERY IMPORTANT - intentional double-lock checking
		//		The other option would be to leverage `java.util.concurrent.locks.ReadWriteLock`
		//		to protect access.  However, synchronized is much simpler here.  We will verify
		// 		during throughput testing whether this is an issue and consider changes then

		CacheableSqmInterpretation localCopy = cacheableSqmInterpretation;
		JdbcParameterBindings jdbcParameterBindings = null;

		if ( localCopy == null ) {
			synchronized ( this ) {
				localCopy = cacheableSqmInterpretation;
				if ( localCopy == null ) {
					localCopy = buildCacheableSqmInterpretation( sqm, domainParameterXref, executionContext );
					jdbcParameterBindings = localCopy.firstParameterBindings;
					localCopy.firstParameterBindings = null;
					cacheableSqmInterpretation = localCopy;
				}
			}
		}
		else {
			// If the translation depends on parameter bindings, or it isn't compatible with the current query options,
			// we have to rebuild the JdbcSelect, which is still better than having to translate from SQM to SQL AST again
			if ( localCopy.jdbcSelect.dependsOnParameterBindings() ) {
				jdbcParameterBindings = createJdbcParameterBindings( localCopy, executionContext );
			}
			// If the translation depends on the limit or lock options, we have to rebuild the JdbcSelect
			// We could avoid this by putting the lock options into the cache key
			if ( !localCopy.jdbcSelect.isCompatibleWith( jdbcParameterBindings, executionContext.getQueryOptions() ) ) {
				localCopy = buildCacheableSqmInterpretation( sqm, domainParameterXref, executionContext );
				jdbcParameterBindings = localCopy.firstParameterBindings;
				localCopy.firstParameterBindings = null;
				cacheableSqmInterpretation = localCopy;
			}
		}

		if ( jdbcParameterBindings == null ) {
			jdbcParameterBindings = createJdbcParameterBindings( localCopy, executionContext );
		}

		return interpreter.interpret( context, executionContext, localCopy, jdbcParameterBindings );
	}

	private JdbcParameterBindings createJdbcParameterBindings(CacheableSqmInterpretation sqmInterpretation, DomainQueryExecutionContext executionContext) {
		final SharedSessionContractImplementor session = executionContext.getSession();
		return SqmUtil.createJdbcParameterBindings(
				executionContext.getQueryParameterBindings(),
				domainParameterXref,
				sqmInterpretation.getJdbcParamsXref(),
				session.getFactory().getRuntimeMetamodels().getMappingMetamodel(),
				sqmInterpretation.getTableGroupAccess()::findTableGroup,
				new SqmParameterMappingModelResolutionAccess() {
					@Override
					@SuppressWarnings("unchecked")
					public <T> MappingModelExpressible<T> getResolvedMappingModelType(SqmParameter<T> parameter) {
						return (MappingModelExpressible<T>) sqmInterpretation.getSqmParameterMappingModelTypes().get( parameter );
					}
				},
				session
		);
	}

	private static CacheableSqmInterpretation buildCacheableSqmInterpretation(
			SqmSelectStatement<?> sqm,
			DomainParameterXref domainParameterXref,
			DomainQueryExecutionContext executionContext) {
		final SharedSessionContractImplementor session = executionContext.getSession();
		final SessionFactoryImplementor sessionFactory = session.getFactory();
		final QueryEngine queryEngine = sessionFactory.getQueryEngine();

		final SqmTranslatorFactory sqmTranslatorFactory = queryEngine.getSqmTranslatorFactory();

		final SqmTranslator<SelectStatement> sqmConverter = sqmTranslatorFactory.createSelectTranslator(
				sqm,
				executionContext.getQueryOptions(),
				domainParameterXref,
				executionContext.getQueryParameterBindings(),
				executionContext.getSession().getLoadQueryInfluencers(),
				sessionFactory,
				true
		);

//			tableGroupAccess = sqmConverter.getFromClauseAccess();
		final SqmTranslation<SelectStatement> sqmInterpretation = sqmConverter.translate();
		final FromClauseAccess tableGroupAccess = sqmConverter.getFromClauseAccess();
		final JdbcServices jdbcServices = sessionFactory.getJdbcServices();
		final JdbcEnvironment jdbcEnvironment = jdbcServices.getJdbcEnvironment();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = jdbcEnvironment.getSqlAstTranslatorFactory();
		final SqlAstTranslator<JdbcOperationQuerySelect> selectTranslator = sqlAstTranslatorFactory
				.buildSelectTranslator( sessionFactory, sqmInterpretation.getSqlAst() );
		final Map<QueryParameterImplementor<?>, Map<SqmParameter<?>, List<JdbcParametersList>>> jdbcParamsXref = SqmUtil
				.generateJdbcParamsXref( domainParameterXref, sqmInterpretation::getJdbcParamsBySqmParam );
		final JdbcParameterBindings jdbcParameterBindings = SqmUtil.createJdbcParameterBindings(
				executionContext.getQueryParameterBindings(),
				domainParameterXref,
				jdbcParamsXref,
				session.getFactory().getRuntimeMetamodels().getMappingMetamodel(),
				tableGroupAccess::findTableGroup,
				new SqmParameterMappingModelResolutionAccess() {
					@Override
					@SuppressWarnings("unchecked")
					public <T> MappingModelExpressible<T> getResolvedMappingModelType(SqmParameter<T> parameter) {
						return (MappingModelExpressible<T>) sqmInterpretation
								.getSqmParameterMappingModelTypeResolutions()
								.get( parameter );
					}
				},
				session
		);
		final JdbcOperationQuerySelect jdbcSelect = selectTranslator.translate(
				jdbcParameterBindings,
				executionContext.getQueryOptions()
		);

		return new CacheableSqmInterpretation(
				sqmInterpretation.getSqlAst(),
				jdbcSelect,
				tableGroupAccess,
				jdbcParamsXref,
				sqmInterpretation.getSqmParameterMappingModelTypeResolutions(),
				jdbcParameterBindings
		);
	}

	private interface SqmInterpreter<T, X> {
		CompletionStage<T> interpret(
				X context,
				DomainQueryExecutionContext executionContext,
				CacheableSqmInterpretation sqmInterpretation,
				JdbcParameterBindings jdbcParameterBindings);
	}

	private static class CacheableSqmInterpretation {
		private final SelectStatement selectStatement;
		private final JdbcOperationQuerySelect jdbcSelect;
		private final FromClauseAccess tableGroupAccess;
		private final Map<QueryParameterImplementor<?>, Map<SqmParameter<?>, List<JdbcParametersList>>> jdbcParamsXref;
		private final Map<SqmParameter<?>, MappingModelExpressible<?>> sqmParameterMappingModelTypes;
		private transient JdbcParameterBindings firstParameterBindings;

		CacheableSqmInterpretation(
				SelectStatement selectStatement,
				JdbcOperationQuerySelect jdbcSelect,
				FromClauseAccess tableGroupAccess,
				Map<QueryParameterImplementor<?>, Map<SqmParameter<?>, List<JdbcParametersList>>> jdbcParamsXref,
				Map<SqmParameter<?>, MappingModelExpressible<?>> sqmParameterMappingModelTypes,
				JdbcParameterBindings firstParameterBindings) {
			this.selectStatement = selectStatement;
			this.jdbcSelect = jdbcSelect;
			this.tableGroupAccess = tableGroupAccess;
			this.jdbcParamsXref = jdbcParamsXref;
			this.sqmParameterMappingModelTypes = sqmParameterMappingModelTypes;
			this.firstParameterBindings = firstParameterBindings;
		}

		SelectStatement getSelectStatement() {
			return selectStatement;
		}

		JdbcOperationQuerySelect getJdbcSelect() {
			return jdbcSelect;
		}

		FromClauseAccess getTableGroupAccess() {
			return tableGroupAccess;
		}

		Map<QueryParameterImplementor<?>, Map<SqmParameter<?>, List<JdbcParametersList>>> getJdbcParamsXref() {
			return jdbcParamsXref;
		}

		public Map<SqmParameter<?>, MappingModelExpressible<?>> getSqmParameterMappingModelTypes() {
			return sqmParameterMappingModelTypes;
		}

		JdbcParameterBindings getFirstParameterBindings() {
			return firstParameterBindings;
		}

		void setFirstParameterBindings(JdbcParameterBindings firstParameterBindings) {
			this.firstParameterBindings = firstParameterBindings;
		}
	}
}
