/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.TypeMismatchException;
import org.hibernate.hql.internal.QueryExecutionRequestException;
import org.hibernate.query.criteria.internal.compile.InterpretedParameterMetadata;
import org.hibernate.query.internal.AbstractProducedQuery;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import jakarta.persistence.EntityGraph;
import jakarta.persistence.NoResultException;
import jakarta.persistence.Parameter;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

/**
 * An internal contract between the reactive session implementation
 * and the {@link org.hibernate.reactive.stage.Stage.Query} and
 * {@link org.hibernate.reactive.mutiny.Mutiny.Query} APIs.
 *
 * @see ReactiveSession
 */
@Incubating
public interface ReactiveQuery<R> {

	void setParameterMetadata(InterpretedParameterMetadata parameterMetadata);

	CompletionStage<R> getReactiveSingleResult();

	CompletionStage<List<R>> getReactiveResultList();

	CompletionStage<R> getReactiveSingleResultOrNull();

	CompletionStage<Integer> executeReactiveUpdate();

	ReactiveQuery<R> setParameter(int position, Object value);

	ReactiveQuery<R> setParameter(String name, Object value);

	<T> ReactiveQuery<R> setParameter(Parameter<T> parameter, T value);

	ReactiveQuery<R> setMaxResults(int maxResults);

	ReactiveQuery<R> setFirstResult(int firstResult);

	int getMaxResults();

	int getFirstResult();

	ReactiveQuery<R> setReadOnly(boolean readOnly);

	boolean isReadOnly();

	ReactiveQuery<R> setComment(String comment);

	ReactiveQuery<R> setQueryHint(String hintName, Object value);

	ReactiveQuery<R> setLockMode(LockMode lockMode);

	ReactiveQuery<R> setLockMode(String alias, LockMode lockMode);

	ReactiveQuery<R> setLockOptions(LockOptions lockOptions);

	ReactiveQuery<R> setCacheMode(CacheMode cacheMode);

	CacheMode getCacheMode();

	FlushMode getHibernateFlushMode();

	ReactiveQuery<R> setHibernateFlushMode(FlushMode flushMode);

	ReactiveQuery<R> setResultTransformer(ResultTransformer resultTransformer);

	Type[] getReturnTypes();

	static <T> T convertQueryException(T result, Throwable e,
									   AbstractProducedQuery<?> query) {
		if ( e instanceof QueryExecutionRequestException) {
			throw new IllegalStateException( e );
		}
		if ( e instanceof TypeMismatchException) {
			throw new IllegalStateException( e );
		}
		if ( e instanceof HibernateException) {
			throw query.getProducer().getExceptionConverter()
					.convert( (HibernateException) e, query.getLockOptions() );
		}
		return returnOrRethrow( e, result );
	}

	static <R> R extractUniqueResult(List<R> list, AbstractProducedQuery<R> query) {
		try {
			if ( list.isEmpty() ) {
				throw new NoResultException( "No entity found for query" );
			}
			return AbstractProducedQuery.uniqueElement( list );
		}
		catch (HibernateException e) {
			throw query.getProducer().getExceptionConverter()
					.convert( e, query.getLockOptions() );
		}
	}

	static <R> R extractUniqueResultOrNull(List<R> list, AbstractProducedQuery<R> query) {
		try {
			if ( list.isEmpty() ) {
				return null;
			}
			return AbstractProducedQuery.uniqueElement( list );
		}
		catch (HibernateException e) {
			throw query.getProducer().getExceptionConverter()
					.convert( e, query.getLockOptions() );
		}
	}

	void setPlan(EntityGraph<R> entityGraph);

    ReactiveQuery<R> setCacheable(boolean cacheable);

	boolean isCacheable();

	ReactiveQuery<R> setCacheRegion(String cacheRegion);

	String getCacheRegion();

	ReactiveQuery<R> setQuerySpaces(String[] querySpaces);
}
