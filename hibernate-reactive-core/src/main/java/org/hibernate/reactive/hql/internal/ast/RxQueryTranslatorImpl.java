package org.hibernate.reactive.hql.internal.ast;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.hibernate.HibernateException;
import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.hql.internal.QueryExecutionRequestException;
import org.hibernate.hql.internal.ast.HqlSqlWalker;
import org.hibernate.hql.internal.ast.QueryTranslatorImpl;
import org.hibernate.hql.internal.ast.tree.QueryNode;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.loader.hql.QueryLoader;
import org.hibernate.reactive.hql.RxQueryLoader;
import org.hibernate.reactive.impl.RxQueryExecutor;
import org.hibernate.reactive.util.impl.RxUtil;

import org.jboss.logging.Logger;

public class RxQueryTranslatorImpl extends QueryTranslatorImpl {

	private RxQueryLoader queryLoader;

	private static final CoreMessageLogger LOG = Logger.getMessageLogger(
			CoreMessageLogger.class,
			RxQueryTranslatorImpl.class.getName()
	);

	public RxQueryTranslatorImpl(
			String queryIdentifier,
			String query,
			Map enabledFilters,
			SessionFactoryImplementor factory) {
		super( queryIdentifier, query, enabledFilters, factory );
	}

	public RxQueryTranslatorImpl(
			String queryIdentifier,
			String query,
			Map enabledFilters,
			SessionFactoryImplementor factory, EntityGraphQueryHint entityGraphQueryHint) {
		super( queryIdentifier, query, enabledFilters, factory, entityGraphQueryHint );
	}

	@Override
	protected QueryLoader createQueryLoader(HqlSqlWalker w, SessionFactoryImplementor factory) {
		this.queryLoader = new RxQueryLoader( this, factory, w.getSelectClause() );
		return queryLoader;
	}

	/**
	 * @deprecated Use {@link #list(SharedSessionContractImplementor, QueryParameters)} instead.
	 */
	@Deprecated
	@Override
	public List<Object> list(SharedSessionContractImplementor session, QueryParameters queryParameters)
			throws HibernateException {
		throw new UnsupportedOperationException("Use #rxList instead");
	}

	public CompletionStage<List<Object>> rxList(SharedSessionContractImplementor session, QueryParameters queryParameters) throws HibernateException {
		// Delegate to the QueryLoader...
		errorIfDML();

		final QueryNode query = (QueryNode) getSqlAST();
		final boolean hasLimit = queryParameters.getRowSelection() != null && queryParameters.getRowSelection().definesLimits();
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
		return queryLoader.rxList( (SessionImplementor) session, queryParametersToUse )
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

	public CompletionStage<Integer> executeRxUpdate(QueryParameters queryParameters, SharedSessionContractImplementor session) {
		final AtomicInteger atomicCounter = new AtomicInteger( 0 );
		CompletionStage<Void> updateStage = RxUtil.nullFuture();
		String[] sqlStatements = getSqlStatements();
		for ( String sql : sqlStatements ) {
			updateStage = updateStage
					.thenCompose( v -> new RxQueryExecutor().update( sql, queryParameters.getPositionalParameterValues(), session.getFactory() )
					.thenAccept( updateCount -> atomicCounter.addAndGet( updateCount ) )
			);
		}
		return updateStage.thenApply( v -> atomicCounter.get() );
	}

	/**
	 * @deprecated Use {@link #executeRxUpdate(QueryParameters queryParameters, SharedSessionContractImplementor session)}
	 */
	@Deprecated
	@Override
	public int executeUpdate(QueryParameters queryParameters, SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException("Use executeRxUpdate instead ");
	}

	//TODO: Change scope in ORM
	private void errorIfSelect() throws HibernateException {
		if ( !getSqlAST().needsExecutor() ) {
			throw new QueryExecutionRequestException( "Not supported for select queries", getQueryString() );
		}
	}
}
