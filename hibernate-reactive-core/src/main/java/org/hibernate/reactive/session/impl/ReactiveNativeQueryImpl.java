package org.hibernate.reactive.session.impl;

import org.hibernate.CacheMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.TypeMismatchException;
import org.hibernate.engine.query.spi.sql.NativeSQLQueryReturn;
import org.hibernate.engine.query.spi.sql.NativeSQLQuerySpecification;
import org.hibernate.engine.spi.NamedSQLQueryDefinition;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.hql.internal.QueryExecutionRequestException;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.internal.NativeQueryImpl;
import org.hibernate.reactive.session.ReactiveNativeQuery;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;

import javax.persistence.NoResultException;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 *  Implementation of {@link ReactiveNativeQuery} by extension of
 *  {@link NativeQueryImpl}.
 *
 * @author Gavin King
 */
public class ReactiveNativeQueryImpl<R> extends NativeQueryImpl<R> implements ReactiveNativeQuery<R> {

	public ReactiveNativeQueryImpl(
			NamedSQLQueryDefinition queryDef,
			SharedSessionContractImplementor session,
			ParameterMetadata parameterMetadata) {
		super(queryDef, session, parameterMetadata);
	}

	public ReactiveNativeQueryImpl(
			String sqlString,
			boolean callable,
			SharedSessionContractImplementor session,
			ParameterMetadata sqlParameterMetadata) {
		super( sqlString, callable, session, sqlParameterMetadata );
	}

	@Override
	public ReactiveNativeQueryImpl<R> setResultTransformer(ResultTransformer nativeQueryTupleTransformer) {
		super.setResultTransformer(nativeQueryTupleTransformer);
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addEntity(String alias, String name, LockMode read) {
		super.addEntity(alias, name, read);
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setResultSetMapping(String name) {
		super.setResultSetMapping(name);
		return this;
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
		return getReactiveResultList().thenApply( this::uniqueResult );
	}

	private R uniqueResult(List<R> list) {
		try {
			if ( list.size() == 0 ) {
				throw new NoResultException( "No entity found for query" );
			}
			return uniqueElement( list );
		}
		catch (HibernateException e) {
			throw getExceptionConverter().convert( e, getLockOptions() );
		}
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate() {
		getProducer().checkTransactionNeededForUpdateOperation( "Executing an update/delete query" );

		beforeQuery();
		return doExecuteReactiveUpdate()
				.handle( (count, e) -> {
					afterQuery();
					if ( e instanceof QueryExecutionRequestException ) {
						CompletionStages.rethrow( new IllegalStateException( e ) );
					}
					if ( e instanceof TypeMismatchException ) {
						CompletionStages.rethrow( new IllegalStateException( e ) );
					}
					if ( e instanceof HibernateException ) {
						CompletionStages.rethrow( getExceptionConverter().convert( (HibernateException) e ) );
					}
					CompletionStages.rethrowIfNotNull( e );
					return count;
				} );
	}

	private CompletionStage<Integer> doExecuteReactiveUpdate() {
		final String expandedQuery = getQueryParameterBindings().expandListValuedParameters( getQueryString(), getProducer() );
		return reactiveProducer().executeReactiveUpdate( expandedQuery, makeQueryParametersForExecution( expandedQuery ) );
	}

	@Override
	public CompletionStage<List<R>> getReactiveResultList() {
		return reactiveList();
	}

	private CompletionStage<List<R>> reactiveList() {
		return reactiveProducer().reactiveList(
				generateQuerySpecification(),
				getQueryParameters()
		);
	}

	private NativeSQLQuerySpecification generateQuerySpecification() {
		return new NativeSQLQuerySpecification(
				getQueryParameterBindings().expandListValuedParameters( getQueryString(), getProducer() ),
				getQueryReturns().toArray( new NativeSQLQueryReturn[getQueryReturns().size()] ),
				getSynchronizedQuerySpaces()
		);
	}

	private ReactiveSession reactiveProducer() {
		return (ReactiveSession) getProducer();
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(int position, Object value) {
		super.setParameter(position, value);
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setMaxResults(int maxResults) {
		super.setMaxResults(maxResults);
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setFirstResult(int firstResult) {
		super.setFirstResult(firstResult);
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setReadOnly(boolean readOnly) {
		super.setReadOnly(readOnly);
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setComment(String comment) {
		super.setComment(comment);
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setLockMode(String alias, LockMode lockMode) {
		super.setLockMode(alias, lockMode);
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setCacheMode(CacheMode cacheMode) {
		super.setCacheMode(cacheMode);
		return this;
	}

}
