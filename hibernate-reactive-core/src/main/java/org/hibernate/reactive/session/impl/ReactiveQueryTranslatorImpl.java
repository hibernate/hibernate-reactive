/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import antlr.RecognitionException;
import antlr.collections.AST;
import org.hibernate.HibernateException;
import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.hql.internal.QueryExecutionRequestException;
import org.hibernate.hql.internal.ast.HqlSqlWalker;
import org.hibernate.hql.internal.ast.QuerySyntaxException;
import org.hibernate.hql.internal.ast.QueryTranslatorImpl;
import org.hibernate.hql.internal.ast.SqlGenerator;
import org.hibernate.hql.internal.ast.tree.QueryNode;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.loader.hql.QueryLoader;
import org.hibernate.param.ParameterSpecification;
import org.hibernate.reactive.adaptor.impl.QueryParametersAdaptor;
import org.hibernate.reactive.loader.hql.impl.ReactiveQueryLoader;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.sql.impl.Parameters.processParameters;

public class ReactiveQueryTranslatorImpl extends QueryTranslatorImpl {

	private ReactiveQueryLoader queryLoader;

	private static final CoreMessageLogger LOG = Logger.getMessageLogger(
			CoreMessageLogger.class,
			ReactiveQueryTranslatorImpl.class.getName()
	);

	public ReactiveQueryTranslatorImpl(
			String queryIdentifier,
			String query,
			Map enabledFilters,
			SessionFactoryImplementor factory) {
		super( queryIdentifier, query, enabledFilters, factory );
	}

	public ReactiveQueryTranslatorImpl(
			String queryIdentifier,
			String query,
			Map enabledFilters,
			SessionFactoryImplementor factory, EntityGraphQueryHint entityGraphQueryHint) {
		super( queryIdentifier, query, enabledFilters, factory, entityGraphQueryHint );
	}

	@Override
	protected QueryLoader createQueryLoader(HqlSqlWalker w, SessionFactoryImplementor factory) {
		this.queryLoader = new ReactiveQueryLoader( this, factory, w.getSelectClause() );
		return queryLoader;
	}

	/**
	 * @deprecated Use {@link #list(SharedSessionContractImplementor, QueryParameters)} instead.
	 */
	@Deprecated
	@Override
	public List<Object> list(SharedSessionContractImplementor session, QueryParameters queryParameters)
			throws HibernateException {
		throw new UnsupportedOperationException("Use #reactiveList instead");
	}

	public CompletionStage<List<Object>> reactiveList(SharedSessionContractImplementor session, QueryParameters queryParameters) throws HibernateException {
		// Delegate to the QueryLoader...
		errorIfDML();

		final QueryNode query = (QueryNode) getSqlAST();
		final boolean hasLimit =
				queryParameters.getRowSelection() != null
						&& queryParameters.getRowSelection().definesLimits();
		final boolean needsDistincting =
				( query.getSelectClause().isDistinct() || getEntityGraphQueryHint() != null || hasLimit )
						&& containsCollectionFetches();

		QueryParameters queryParametersToUse;
		if ( hasLimit && containsCollectionFetches() ) {
			boolean fail = session.getFactory().getSessionFactoryOptions().isFailOnPaginationOverCollectionFetchEnabled();
			if (fail) {
				throw new HibernateException("firstResult/maxResults specified with collection fetch. " +
													 "In memory pagination was about to be applied. " +
													 "Failing because 'Fail on pagination over collection fetch' is enabled.");
			}
			else {
				LOG.firstOrMaxResultsSpecifiedWithCollectionFetch();
			}
			RowSelection selection = new RowSelection();
			selection.setFetchSize( queryParameters.getRowSelection().getFetchSize() );
			selection.setTimeout( queryParameters.getRowSelection().getTimeout() );
			queryParametersToUse = queryParameters.createCopyUsing( selection );
		}
		else {
			queryParametersToUse = queryParameters;
		}

		return queryLoader.reactiveList( (SessionImplementor) session, queryParametersToUse )
				.thenApply(results -> {
					if ( needsDistincting ) {
						int includedCount = -1;
						// NOTE : firstRow is zero-based
						int first = !hasLimit || queryParameters.getRowSelection().getFirstRow() == null
								? 0
								: queryParameters.getRowSelection().getFirstRow();
						int max = !hasLimit || queryParameters.getRowSelection().getMaxRows() == null
								? -1
								: queryParameters.getRowSelection().getMaxRows();
						List<Object> tmp = new ArrayList<>();
						IdentitySet distinction = new IdentitySet();
						for ( final Object result : results ) {
							if ( !distinction.add( result ) ) {
								continue;
							}
							includedCount++;
							if ( includedCount < first ) {
								continue;
							}
							tmp.add( result );
							// NOTE : ( max - 1 ) because first is zero-based while max is not...
							if ( max >= 0 && ( includedCount - first ) >= ( max - 1 ) ) {
								break;
							}
						}
						return tmp;
					}
					return results;
				});
	}

	public CompletionStage<Integer> executeReactiveUpdate(QueryParameters queryParameters, ReactiveSession session) {
		errorIfSelect();

		// Multiple UPDATE SQL strings are not supported yet

		final String processedSql = processParameters( getSqlStatements()[0], session.getDialect() );
		final Object[] parameterValues = QueryParametersAdaptor.toParameterArray(
				queryParameters,
				getCollectedParameterSpecifications( session ),
				session.getSharedContract()
		);
		return CompletionStages.completedFuture(0)
				.thenCompose( count -> session.getReactiveConnection()
						.update( processedSql, parameterValues )
						.thenApply( updateCount -> count + updateCount )
				);
	}

	/**
	 * @deprecated Use {@link #executeReactiveUpdate(QueryParameters queryParameters, ReactiveSession session)}
	 */
	@Deprecated
	@Override
	public int executeUpdate(QueryParameters queryParameters, SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException("Use executeReactiveUpdate instead ");
	}

	// TODO: it would be nice to be able to override getCollectedParameterSpecifications().
	//       To do that, we would need to add protected method, QueryTranslatorImpl#getFactory
	private List<ParameterSpecification> getCollectedParameterSpecifications(ReactiveSession session) {
		// Currently, ORM returns null for getCollectedParameterSpecifications() a StatementExecute
		List<ParameterSpecification> parameterSpecifications = getCollectedParameterSpecifications();
		if ( parameterSpecifications == null ) {
			final SqlGenerator gen = new SqlGenerator( session.getFactory() );
			try {
				gen.statement( (AST) getSqlAST() );
				parameterSpecifications = gen.getCollectedParameters();
			} catch (RecognitionException e) {
				throw QuerySyntaxException.convert(e);
			}
		}
		return parameterSpecifications;
	}

	//TODO: Change scope in ORM
	private void errorIfSelect() throws HibernateException {
		if ( !getSqlAST().needsExecutor() ) {
			throw new QueryExecutionRequestException( "Not supported for select queries", getQueryString() );
		}
	}
}
