/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.spi;

import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.reactive.query.ReactiveQueryImplementor;
import org.hibernate.reactive.query.ReactiveSelectionQuery;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.Type;

/**
 * An internal contract for a reactive selection query.
 * Mirrors {@link org.hibernate.query.spi.SelectionQueryImplementor}.
 *
 * @param <R> the result type
 */
public interface ReactiveSelectionQueryImplementor<R> extends ReactiveQueryImplementor<R>, ReactiveSelectionQuery<R> {

	@Override
	<T> ReactiveSelectionQueryImplementor<T> setTupleTransformer(TupleTransformer<T> transformer);

	@Override
	ReactiveSelectionQueryImplementor<R> setResultListTransformer(ResultListTransformer<R> transformer);

	@Override
	ReactiveSelectionQueryImplementor<R> setHibernateFlushMode(FlushMode flushMode);

	@Override
	ReactiveSelectionQueryImplementor<R> setFlushMode(FlushModeType flushMode);

	@Override
	ReactiveSelectionQueryImplementor<R> setTimeout(int timeout);

	@Override
	ReactiveSelectionQueryImplementor<R> setFetchSize(int fetchSize);

	@Override
	ReactiveSelectionQueryImplementor<R> setReadOnly(boolean readOnly);

	@Override
	ReactiveSelectionQueryImplementor<R> setMaxResults(int maxResult);

	@Override
	ReactiveSelectionQueryImplementor<R> setFirstResult(int startPosition);

	@Override
	ReactiveSelectionQueryImplementor<R> setCacheMode(CacheMode cacheMode);

	@Override
	ReactiveSelectionQueryImplementor<R> setCacheStoreMode(CacheStoreMode cacheStoreMode);

	@Override
	ReactiveSelectionQueryImplementor<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode);

	@Override
	ReactiveSelectionQueryImplementor<R> setCacheable(boolean cacheable);

	@Override
	ReactiveSelectionQueryImplementor<R> setCacheRegion(String cacheRegion);

	@Override
	ReactiveSelectionQueryImplementor<R> setHint(String hintName, Object value);

	@Override
	ReactiveSelectionQueryImplementor<R> setLockMode(LockModeType lockMode);

	@Override
	ReactiveSelectionQueryImplementor<R> setHibernateLockMode(LockMode lockMode);

	@Override
	ReactiveSelectionQueryImplementor<R> setLockMode(String alias, LockMode lockMode);

	@Override
	ReactiveSelectionQueryImplementor<R> setFollowOnLocking(boolean enable);

	@Override
	ReactiveSelectionQueryImplementor<R> enableFetchProfile(String profileName);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameter(String name, Object value);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameter(String name, P value, Class<P> type);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameter(String name, P value, Type<P> type);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameter(String name, Instant value, TemporalType temporalType);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameter(String name, Calendar value, TemporalType temporalType);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameter(String name, Date value, TemporalType temporalType);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameter(int position, Object value);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameter(int position, P value, Class<P> type);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameter(int position, P value, Type<P> type);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameter(int position, Instant value, TemporalType temporalType);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameter(int position, Calendar value, TemporalType temporalType);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameter(int position, Date value, TemporalType temporalType);

	@Override
	<T> ReactiveSelectionQueryImplementor<R> setParameter(QueryParameter<T> parameter, T value);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameter(QueryParameter<P> parameter, P value, Class<P> type);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameter(QueryParameter<P> parameter, P val, Type<P> type);

	@Override
	<T> ReactiveSelectionQueryImplementor<R> setParameter(Parameter<T> param, T value);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameterList(String name, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(String name, Collection<? extends P> values, Type<P> type);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameterList(String name, Object[] values);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(String name, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(String name, P[] values, Type<P> type);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(int position, Collection<? extends P> values, Type<P> type);

	@Override
	ReactiveSelectionQueryImplementor<R> setParameterList(int position, Object[] values);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(int position, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(int position, P[] values, Type<P> type);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Type<P> type);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values, Type<P> type);

	@Override
	ReactiveSelectionQueryImplementor<R> setProperties(Object bean);

	@Override
	ReactiveSelectionQueryImplementor<R> setProperties(@SuppressWarnings("rawtypes") Map bean);
}
