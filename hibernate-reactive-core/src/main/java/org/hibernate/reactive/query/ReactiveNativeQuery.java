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
import java.util.Map;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.metamodel.model.domain.BasicDomainType;
import org.hibernate.query.BindableType;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.NativeQuery.FetchReturn;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.type.BasicTypeReference;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.SingularAttribute;

/**
 * @see org.hibernate.query.NativeQuery
 */
public interface ReactiveNativeQuery<R> extends ReactiveQuery<R> {
	ReactiveNativeQuery<R> addScalar(String columnAlias);

	ReactiveNativeQuery<R> addScalar(String columnAlias, @SuppressWarnings("rawtypes") BasicTypeReference type);

	ReactiveNativeQuery<R> addScalar(String columnAlias, @SuppressWarnings("rawtypes") BasicDomainType type);
	ReactiveNativeQuery<R> addScalar(String columnAlias, @SuppressWarnings("rawtypes") Class javaType);

	<C> ReactiveNativeQuery<R> addScalar(String columnAlias, Class<C> relationalJavaType, AttributeConverter<?,C> converter);

	<O,T> ReactiveNativeQuery<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<T> jdbcJavaType, AttributeConverter<O,T> converter);

	<C> ReactiveNativeQuery<R> addScalar(String columnAlias, Class<C> relationalJavaType, Class<? extends AttributeConverter<?,C>> converter);

	<O,T> ReactiveNativeQuery<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<T> jdbcJavaType, Class<? extends AttributeConverter<O,T>> converter);

	<J> NativeQuery.InstantiationResultNode<J> addInstantiation(Class<J> targetJavaType);

	ReactiveNativeQuery<R> addAttributeResult(String columnAlias, @SuppressWarnings("rawtypes") Class entityJavaType, String attributePath);

	ReactiveNativeQuery<R> addAttributeResult(String columnAlias, String entityName, String attributePath);

	ReactiveNativeQuery<R> addAttributeResult(String columnAlias, @SuppressWarnings("rawtypes") SingularAttribute attribute);

	NativeQuery.RootReturn addRoot(String tableAlias, String entityName);

	NativeQuery.RootReturn addRoot(String tableAlias, @SuppressWarnings("rawtypes") Class entityType);

	ReactiveNativeQuery<R> addEntity(String entityName);

	ReactiveNativeQuery<R> addEntity(String tableAlias, String entityName);

	ReactiveNativeQuery<R> addEntity(String tableAlias, String entityName, LockMode lockMode);

	ReactiveNativeQuery<R> addEntity(@SuppressWarnings("rawtypes") Class entityType);

	ReactiveNativeQuery<R> addEntity(String tableAlias, @SuppressWarnings("rawtypes") Class entityType);

	ReactiveNativeQuery<R> addEntity(String tableAlias, @SuppressWarnings("rawtypes") Class entityClass, LockMode lockMode);

	FetchReturn addFetch(String tableAlias, String ownerTableAlias, String joinPropertyName);

	ReactiveNativeQuery<R> addJoin(String tableAlias, String path);

	ReactiveNativeQuery<R> addJoin(String tableAlias, String ownerTableAlias, String joinPropertyName);

	ReactiveNativeQuery<R> addJoin(String tableAlias, String path, LockMode lockMode);

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// covariant overrides - Query

	@Override
	ReactiveNativeQuery<R> setHibernateFlushMode(FlushMode flushMode);

	@Override
	ReactiveNativeQuery<R> setFlushMode(FlushModeType flushMode);

	@Override
	ReactiveNativeQuery<R> setCacheMode(CacheMode cacheMode);

	@Override
	ReactiveNativeQuery<R> setCacheStoreMode(CacheStoreMode cacheStoreMode);

	@Override
	ReactiveNativeQuery<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode);

	@Override
	ReactiveNativeQuery<R> setCacheable(boolean cacheable);

	@Override
	ReactiveNativeQuery<R> setCacheRegion(String cacheRegion);

	@Override
	ReactiveNativeQuery<R> setTimeout(int timeout);

	@Override
	ReactiveNativeQuery<R> setFetchSize(int fetchSize);

	@Override
	ReactiveNativeQuery<R> setReadOnly(boolean readOnly);

	/**
	 * @inheritDoc
	 *
	 * This operation is supported even for native queries.
	 * Note that specifying an explicit lock mode might
	 * result in changes to the native SQL query that is
	 * actually executed.
	 */
	@Override
	LockOptions getLockOptions();

	/**
	 * @inheritDoc
	 *
	 * This operation is supported even for native queries.
	 * Note that specifying an explicit lock mode might
	 * result in changes to the native SQL query that is
	 * actually executed.
	 */
	@Override
	ReactiveNativeQuery<R> setLockOptions(LockOptions lockOptions);

	/**
	 * Not applicable to native SQL queries.
	 *
	 * @throws IllegalStateException for consistency with JPA
	 */
	@Override
	ReactiveNativeQuery<R> setLockMode(String alias, LockMode lockMode);

	@Override
	ReactiveNativeQuery<R> setComment(String comment);

	@Override
	ReactiveNativeQuery<R> addQueryHint(String hint);

	@Override
	ReactiveNativeQuery<R> setMaxResults(int maxResult);

	@Override
	ReactiveNativeQuery<R> setFirstResult(int startPosition);

	@Override
	ReactiveNativeQuery<R> setHint(String hintName, Object value);

	/**
	 * Not applicable to native SQL queries, due to an unfortunate
	 * requirement of the JPA specification.
	 * <p>
	 * Use {@link #getHibernateLockMode()} to obtain the lock mode.
	 *
	 * @throws IllegalStateException as required by JPA
	 */
	@Override
	LockModeType getLockMode();

	/**
	 * @inheritDoc
	 *
	 * This operation is supported even for native queries.
	 * Note that specifying an explicit lock mode might
	 * result in changes to the native SQL query that is
	 * actually executed.
	 */
	@Override
	LockMode getHibernateLockMode();

	/**
	 * Not applicable to native SQL queries, due to an unfortunate
	 * requirement of the JPA specification.
	 * <p>
	 * Use {@link #setHibernateLockMode(LockMode)} or the hint named
	 * {@value org.hibernate.jpa.HibernateHints#HINT_NATIVE_LOCK_MODE}
	 * to set the lock mode.
	 *
	 * @throws IllegalStateException as required by JPA
	 */
	@Override
	ReactiveNativeQuery<R> setLockMode(LockModeType lockMode);

	/**
	 * @inheritDoc
	 *
	 * This operation is supported even for native queries.
	 * Note that specifying an explicit lock mode might
	 * result in changes to the native SQL query that is
	 * actually executed.
	 */
	@Override
	ReactiveNativeQuery<R> setHibernateLockMode(LockMode lockMode);

	@Override
	<T> ReactiveNativeQuery<T> setTupleTransformer(TupleTransformer<T> transformer);

	@Override
	ReactiveNativeQuery<R> setResultListTransformer(ResultListTransformer<R> transformer);

	@Override
	ReactiveNativeQuery<R> setParameter(String name, Object value);

	@Override
	<P> ReactiveNativeQuery<R> setParameter(String name, P val, Class<P> type);

	@Override
	<P> ReactiveNativeQuery<R> setParameter(String name, P val, BindableType<P> type);

	@Override
	ReactiveNativeQuery<R> setParameter(String name, Instant value, TemporalType temporalType);

	@Override
	ReactiveNativeQuery<R> setParameter(String name, Calendar value, TemporalType temporalType);

	@Override
	ReactiveNativeQuery<R> setParameter(String name, Date value, TemporalType temporalType);

	@Override
	ReactiveNativeQuery<R> setParameter(int position, Object value);

	@Override
	<P> ReactiveNativeQuery<R> setParameter(int position, P val, Class<P> type);

	@Override
	<P> ReactiveNativeQuery<R> setParameter(int position, P val, BindableType<P> type);

	@Override
	ReactiveNativeQuery<R> setParameter(int position, Instant value, TemporalType temporalType);

	@Override
	ReactiveNativeQuery<R> setParameter(int position, Calendar value, TemporalType temporalType);

	@Override
	ReactiveNativeQuery<R> setParameter(int position, Date value, TemporalType temporalType);

	@Override
	<P> ReactiveNativeQuery<R> setParameter(QueryParameter<P> parameter, P val);

	@Override
	<P> ReactiveNativeQuery<R> setParameter(QueryParameter<P> parameter, P val, Class<P> type);

	@Override
	<P> ReactiveNativeQuery<R> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type);

	@Override
	<P> ReactiveNativeQuery<R> setParameter(Parameter<P> param, P value);

	@Override
	ReactiveNativeQuery<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

	@Override
	ReactiveNativeQuery<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

	@Override
	ReactiveNativeQuery<R> setParameterList(String name, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(String name, Collection<? extends P> values, Class<P> type);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(String name, Collection<? extends P> values, BindableType<P> type);

	@Override
	ReactiveNativeQuery<R> setParameterList(String name, Object[] values);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(String name, P[] values, Class<P> type);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(String name, P[] values, BindableType<P> type);

	@Override
	ReactiveNativeQuery<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(int position, Collection<? extends P> values, Class<P> type);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(int position, Collection<? extends P> values, BindableType<P> javaType);

	@Override
	ReactiveNativeQuery<R> setParameterList(int position, Object[] values);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(int position, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(int position, P[] values, BindableType<P> javaType);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, BindableType<P> type);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] values);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveNativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, BindableType<P> type);

	@Override
	ReactiveNativeQuery<R> setProperties(Object bean);

	@Override
	ReactiveNativeQuery<R> setProperties(@SuppressWarnings("rawtypes") Map bean);

}
