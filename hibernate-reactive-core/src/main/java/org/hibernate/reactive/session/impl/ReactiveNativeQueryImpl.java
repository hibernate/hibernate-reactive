/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.engine.query.spi.sql.NativeSQLQueryReturn;
import org.hibernate.engine.query.spi.sql.NativeSQLQuerySpecification;
import org.hibernate.engine.spi.NamedSQLQueryDefinition;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.criteria.internal.compile.InterpretedParameterMetadata;
import org.hibernate.query.internal.NativeQueryImpl;
import org.hibernate.reactive.session.ReactiveNativeQuery;
import org.hibernate.reactive.session.ReactiveQuery;
import org.hibernate.reactive.session.ReactiveQueryExecutor;
import org.hibernate.transform.ResultTransformer;

import javax.persistence.EntityGraph;
import javax.persistence.Parameter;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.session.ReactiveQuery.convertQueryException;
import static org.hibernate.reactive.session.ReactiveQuery.extractUniqueResult;

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
	public void setParameterMetadata(InterpretedParameterMetadata parameterMetadata) {
		throw new UnsupportedOperationException("not a JPA query");
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
		return getReactiveResultList().thenApply( list -> extractUniqueResult( list, this ) );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate() {
		getProducer().checkTransactionNeededForUpdateOperation( "Executing an update/delete query" );

		beforeQuery();
		return reactiveProducer()
				.executeReactiveUpdate( generateQuerySpecification(), getQueryParameters() )
				.whenComplete( (count, error) -> afterQuery() )
				.handle( (count, error) -> convertQueryException( count, error, this ) );
	}

	@Override
	public CompletionStage<List<R>> getReactiveResultList() {
		beforeQuery();
		return reactiveProducer()
				.<R>reactiveList( generateQuerySpecification(), getQueryParameters() )
				.whenComplete( (list, err) -> afterQuery() )
				.handle( (list, error) -> convertQueryException( list, error, this ) );
	}

	private NativeSQLQuerySpecification generateQuerySpecification() {
		return new NativeSQLQuerySpecification(
				getQueryParameterBindings().expandListValuedParameters( getQueryString(), getProducer() ),
				getQueryReturns().toArray( new NativeSQLQueryReturn[0] ),
				getSynchronizedQuerySpaces()
		);
	}

	private ReactiveQueryExecutor reactiveProducer() {
		return (ReactiveQueryExecutor) getProducer();
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(int position, Object value) {
		super.setParameter(position, value);
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(String name, Object value) {
		super.setParameter(name, value);
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameter(Parameter<P> parameter, P value) {
		super.setParameter(parameter, value);
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
	public ReactiveQuery<R> setLockMode(LockMode lockMode) {
		throw new UnsupportedOperationException("LockMode not supported for native SQL queries");
	}

	@Override
	public ReactiveQuery<R> setQueryHint(String hintName, Object value) {
		super.setHint(hintName, value);
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

	@Override
	public FlushMode getHibernateFlushMode() {
		return super.getHibernateFlushMode();
	}

	@Override
	public ReactiveNativeQueryImpl<R> setHibernateFlushMode(FlushMode flushMode) {
		super.setHibernateFlushMode(flushMode);
		return this;
	}

	@Override
	public void setPlan(EntityGraph<R> entityGraph) {
		throw new UnsupportedOperationException("native SQL query cannot have a fetch plan");
	}
}
