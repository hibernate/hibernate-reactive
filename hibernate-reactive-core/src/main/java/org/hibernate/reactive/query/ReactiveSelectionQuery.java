/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query;

import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.query.BindableType;
import org.hibernate.query.CommonQueryContract;
import org.hibernate.query.QueryParameter;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;

/**
 * @see org.hibernate.query.SelectionQuery
 */
public interface ReactiveSelectionQuery<R> extends CommonQueryContract {

	String getQueryString();

	default CompletionStage<List<R>> getReactiveResultList() {
		return reactiveList();
	}

	CompletionStage<List<R>> reactiveList();

	CompletionStage<R> getReactiveSingleResult();

	CompletionStage<R> getReactiveSingleResultOrNull();

	CompletionStage<R> reactiveUnique();

	CompletionStage<Optional<R>> reactiveUniqueResultOptional();

	ReactiveSelectionQuery<R> setHint(String hintName, Object value);

	// Covariant methods

	@Override
	ReactiveSelectionQuery<R> setFlushMode(FlushModeType flushMode);

	@Override
	ReactiveSelectionQuery<R> setHibernateFlushMode(FlushMode flushMode);

	@Override
	ReactiveSelectionQuery<R> setTimeout(int timeout);
	Integer getFetchSize();

	ReactiveSelectionQuery<R> setFetchSize(int fetchSize);

	boolean isReadOnly();

	ReactiveSelectionQuery<R> setReadOnly(boolean readOnly);

	ReactiveSelectionQuery<R> setMaxResults(int maxResult);

	int getFirstResult();

	ReactiveSelectionQuery<R> setFirstResult(int startPosition);

	CacheMode getCacheMode();

	CacheStoreMode getCacheStoreMode();

	CacheRetrieveMode getCacheRetrieveMode();

	ReactiveSelectionQuery<R> setCacheMode(CacheMode cacheMode);

	ReactiveSelectionQuery<R> setCacheStoreMode(CacheStoreMode cacheStoreMode);

	/**
	 * @see #setCacheMode(CacheMode)
	 */
	ReactiveSelectionQuery<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode);

	boolean isCacheable();

	ReactiveSelectionQuery<R> setCacheable(boolean cacheable);

	String getCacheRegion();

	ReactiveSelectionQuery<R> setCacheRegion(String cacheRegion);

	LockOptions getLockOptions();

	LockModeType getLockMode();

	ReactiveSelectionQuery<R> setLockMode(LockModeType lockMode);

	LockMode getHibernateLockMode();

	ReactiveSelectionQuery<R> setHibernateLockMode(LockMode lockMode);

	ReactiveSelectionQuery<R> setLockMode(String alias, LockMode lockMode);

	ReactiveSelectionQuery<R> setAliasSpecificLockMode(String alias, LockMode lockMode);

	ReactiveSelectionQuery<R> setFollowOnLocking(boolean enable);

	@Override
	ReactiveSelectionQuery<R> setParameter(String name, Object value);

	@Override
	<P> ReactiveSelectionQuery<R> setParameter(String name, P value, Class<P> type);

	@Override
	<P> ReactiveSelectionQuery<R> setParameter(String name, P value, BindableType<P> type);

	@Override
	ReactiveSelectionQuery<R> setParameter(String name, Instant value, TemporalType temporalType);

	@Override
	ReactiveSelectionQuery<R> setParameter(String name, Calendar value, TemporalType temporalType);

	@Override
	ReactiveSelectionQuery<R> setParameter(String name, Date value, TemporalType temporalType);

	@Override
	ReactiveSelectionQuery<R> setParameter(int position, Object value);

	@Override
	<P> ReactiveSelectionQuery<R> setParameter(int position, P value, Class<P> type);

	@Override
	<P> ReactiveSelectionQuery<R> setParameter(int position, P value, BindableType<P> type);

	@Override
	ReactiveSelectionQuery<R> setParameter(int position, Instant value, TemporalType temporalType);

	@Override
	ReactiveSelectionQuery<R> setParameter(int position, Date value, TemporalType temporalType);

	@Override
	ReactiveSelectionQuery<R> setParameter(int position, Calendar value, TemporalType temporalType);

	@Override
	<T> ReactiveSelectionQuery<R> setParameter(QueryParameter<T> parameter, T value);

	@Override
	<P> ReactiveSelectionQuery<R> setParameter(QueryParameter<P> parameter, P value, Class<P> type);

	@Override
	<P> ReactiveSelectionQuery<R> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type);

	@Override
	<T> ReactiveSelectionQuery<R> setParameter(Parameter<T> param, T value);

	@Override
	ReactiveSelectionQuery<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

	@Override
	ReactiveSelectionQuery<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

	@Override
	ReactiveSelectionQuery<R> setParameterList(String name, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(String name, Collection<? extends P> values, BindableType<P> type);

	@Override
	ReactiveSelectionQuery<R> setParameterList(String name, Object[] values);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(String name, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(String name, P[] values, BindableType<P> type);

	@Override
	ReactiveSelectionQuery<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(int position, Collection<? extends P> values, BindableType<P> type);

	@Override
	ReactiveSelectionQuery<R> setParameterList(int position, Object[] values);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(int position, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(int position, P[] values, BindableType<P> type);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, BindableType<P> type);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(QueryParameter<P> parameter, P[] values);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSelectionQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, BindableType<P> type);

	@Override
	ReactiveSelectionQuery<R> setProperties(Object bean);

	@Override
	ReactiveSelectionQuery<R> setProperties(@SuppressWarnings("rawtypes") Map bean);
}
