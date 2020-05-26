/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.Filter;
import org.hibernate.HibernateException;
import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.hql.spi.QueryTranslator;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.reactive.util.impl.CompletionStages;

/**
 * A reactific {@link HQLQueryPlan}
 */
class ReactiveHQLQueryPlan extends HQLQueryPlan {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( ReactiveHQLQueryPlan.class );

	public ReactiveHQLQueryPlan(
			String hql,
			boolean shallow,
			Map<String, Filter> enabledFilters,
			SessionFactoryImplementor factory) {
		super( hql, shallow, enabledFilters, factory );
	}

	public ReactiveHQLQueryPlan(
			String hql,
			boolean shallow,
			Map<String, Filter> enabledFilters,
			SessionFactoryImplementor factory, EntityGraphQueryHint entityGraphQueryHint) {
		super( hql, shallow, enabledFilters, factory, entityGraphQueryHint );
	}

	public ReactiveHQLQueryPlan(
			String hql,
			String collectionRole,
			boolean shallow,
			Map<String, Filter> enabledFilters,
			SessionFactoryImplementor factory,
			EntityGraphQueryHint entityGraphQueryHint) {
		super( hql, collectionRole, shallow, enabledFilters, factory, entityGraphQueryHint );
	}

	/**
	 * @deprecated Use performReactiveList instead
	 */
	@Deprecated
	@Override
	public List<Object> performList(
			QueryParameters queryParameters, SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Use performReactiveList instead" );
	}

	/**
	 * @see HQLQueryPlan#performList(QueryParameters, SharedSessionContractImplementor)
	 * @throws HibernateException
	 */
	public CompletionStage<List<Object>> performReactiveList(QueryParameters queryParameters, SharedSessionContractImplementor session) throws HibernateException {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Find: {0}", getSourceQuery() );
			queryParameters.traceParameters( session.getFactory() );
		}

		// NOTE: In the superclass this is a private field.
		// getTranslators() creates a copy of the field array each time.
		final QueryTranslator[] translators = getTranslators();

		final RowSelection rowSelection = queryParameters.getRowSelection();
		final boolean hasLimit = rowSelection != null
				&& rowSelection.definesLimits();
		final boolean needsLimit = hasLimit && translators.length > 1;

		final QueryParameters queryParametersToUse;
		if ( needsLimit ) {
			LOG.needsLimit();
			final RowSelection selection = new RowSelection();
			selection.setFetchSize( queryParameters.getRowSelection().getFetchSize() );
			selection.setTimeout( queryParameters.getRowSelection().getTimeout() );
			queryParametersToUse = queryParameters.createCopyUsing( selection );
		}
		else {
			queryParametersToUse = queryParameters;
		}

		//fast path to avoid unnecessary allocation and copying
		if ( translators.length == 1 ) {
			ReactiveQueryTranslatorImpl reactiveTranslator = (ReactiveQueryTranslatorImpl) translators[0];
			return reactiveTranslator.reactiveList( session, queryParametersToUse );
		}
		final int guessedResultSize = guessResultSize( rowSelection );
		final List<Object> combinedResults = new ArrayList<>( guessedResultSize );
		final IdentitySet distinction;
		if ( needsLimit ) {
			distinction = new IdentitySet( guessedResultSize );
		}
		else {
			distinction = null;
		}
		AtomicInteger includedCount = new AtomicInteger( -1 );
		CompletionStage<Void> combinedStage = CompletionStages.nullFuture();
		for ( QueryTranslator translator : translators ) {
			ReactiveQueryTranslatorImpl reactiveTranslator = (ReactiveQueryTranslatorImpl) translator;
			combinedStage = combinedStage.thenCompose( v -> reactiveTranslator.reactiveList( session, queryParametersToUse ) )
					.thenAccept( tmpList -> {
						if ( needsLimit ) {
							needsLimitLoop( queryParameters, combinedResults, distinction, includedCount, tmpList );
						}
						else {
							combinedResults.addAll( tmpList );
						}
					} );
		}
		return combinedStage.thenApply( ignore -> combinedResults );
	}

	private void needsLimitLoop(QueryParameters queryParameters, List<Object> combinedResults, IdentitySet distinction, AtomicInteger includedCount, List<Object> tmpList) {
		// NOTE : firstRow is zero-based
		final int first = queryParameters.getRowSelection().getFirstRow() == null ? 0 : queryParameters.getRowSelection().getFirstRow();
		final int max = queryParameters.getRowSelection().getMaxRows() == null ? -1 : queryParameters.getRowSelection().getMaxRows();
		for ( final Object result : tmpList ) {
			if ( !distinction.add( result ) ) {
				continue;
			}
			int included = includedCount.addAndGet( 1 );
			if ( included < first ) {
				continue;
			}
			combinedResults.add( result );
			if ( max >= 0 && included > max ) {
				return;
			}
		}
	}

	public CompletionStage<Integer> performExecuteReactiveUpdate(QueryParameters queryParameters, SharedSessionContractImplementor session) {
		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Execute update: {0}", getSourceQuery() );
			queryParameters.traceParameters( session.getFactory() );
		}
		QueryTranslator[] translators = getTranslators();
		if ( translators.length != 1 ) {
			LOG.splitQueries( getSourceQuery(), translators.length );
		}
		AtomicInteger includedCount = new AtomicInteger( 0 );
		CompletionStage<Void> combinedStage = CompletionStages.nullFuture();
		for ( QueryTranslator translator : translators ) {
			ReactiveQueryTranslatorImpl reactiveTranslator = (ReactiveQueryTranslatorImpl) translator;
			combinedStage = combinedStage
					.thenCompose( v -> reactiveTranslator.executeReactiveUpdate( queryParameters, session ) )
					.thenAccept( count -> includedCount.addAndGet( count ) );
		}
		return combinedStage.thenApply( ignore -> includedCount.get() );

	}
}
