/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import org.hibernate.CacheMode;
import org.hibernate.HibernateException;
import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.TypeMismatchException;
import org.hibernate.hql.internal.QueryExecutionRequestException;
import org.hibernate.query.criteria.internal.compile.InterpretedParameterMetadata;
import org.hibernate.query.internal.AbstractProducedQuery;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import javax.persistence.NoResultException;
import javax.persistence.Parameter;
import java.util.List;
import java.util.concurrent.CompletionStage;

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

	CompletionStage<Integer> executeReactiveUpdate();

	ReactiveQuery<R> setParameter(int position, Object value);

	ReactiveQuery<R> setParameter(String name, Object value);

	<T> ReactiveQuery<R> setParameter(Parameter<T> parameter, T value);

	ReactiveQuery<R> setMaxResults(int maxResults);

	ReactiveQuery<R> setFirstResult(int firstResult);

	ReactiveQuery<R> setReadOnly(boolean readOnly);

	ReactiveQuery<R> setComment(String comment);

	ReactiveQuery<R> setLockMode(String alias, LockMode lockMode);

	ReactiveQuery<R> setCacheMode(CacheMode cacheMode);

	CacheMode getCacheMode();

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
		return CompletionStages.returnOrRethrow( e, result );
	}

	static <R> R extractUniqueResult(List<R> list, AbstractProducedQuery<R> query) {
		try {
			if ( list.size() == 0 ) {
				throw new NoResultException( "No entity found for query" );
			}
			return AbstractProducedQuery.uniqueElement( list );
		}
		catch (HibernateException e) {
			throw query.getProducer().getExceptionConverter()
					.convert( e, query.getLockOptions() );
		}
	}
}
