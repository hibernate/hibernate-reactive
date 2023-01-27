/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.spi;

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
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.named.NameableQuery;
import org.hibernate.query.results.dynamic.DynamicResultBuilderEntityStandard;
import org.hibernate.query.sql.spi.NamedNativeQueryMemento;
import org.hibernate.reactive.query.ReactiveNativeQuery;
import org.hibernate.reactive.query.ReactiveQueryImplementor;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.SingularAttribute;

public interface ReactiveNativeQueryImplementor<R> extends ReactiveNativeQuery<R>, ReactiveQueryImplementor<R>, NameableQuery {

	/**
	 * Best guess whether this is a select query.  {@code null}
	 * indicates unknown
	 */
	Boolean isSelectQuery();
	@Override
	NamedNativeQueryMemento toMemento(String name);

	@Override
	ReactiveNativeQueryImplementor<R> addScalar(String columnAlias);

	@Override
	ReactiveNativeQueryImplementor<R> addScalar(String columnAlias, BasicDomainType type);

	@Override
	ReactiveNativeQueryImplementor<R> addScalar(String columnAlias, @SuppressWarnings("rawtypes") Class javaType);

	@Override
	ReactiveNativeQueryImplementor<R> addScalar(int position, Class<?> type);

	@Override
	<C> ReactiveNativeQueryImplementor<R> addScalar(String columnAlias, Class<C> relationalJavaType, AttributeConverter<?,C> converter);

	@Override
	<O, J> ReactiveNativeQueryImplementor<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<J> jdbcJavaType, AttributeConverter<O, J> converter);

	@Override
	<C> ReactiveNativeQueryImplementor<R> addScalar(String columnAlias, Class<C> relationalJavaType, Class<? extends AttributeConverter<?,C>> converter);

	@Override
	<O, J> ReactiveNativeQueryImplementor<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<J> jdbcJavaType, Class<? extends AttributeConverter<O, J>> converter);

	@Override
	ReactiveNativeQueryImplementor<R> addAttributeResult(String columnAlias, @SuppressWarnings("rawtypes") Class entityJavaType, String attributePath);

	@Override
	ReactiveNativeQueryImplementor<R> addAttributeResult(String columnAlias, String entityName, String attributePath);

	@Override
	ReactiveNativeQueryImplementor<R> addAttributeResult(String columnAlias, @SuppressWarnings("rawtypes") SingularAttribute attribute);

	@Override
	DynamicResultBuilderEntityStandard addRoot(String tableAlias, String entityName);

	@Override
	ReactiveNativeQueryImplementor<R> addEntity(String entityName);

	@Override
	ReactiveNativeQueryImplementor<R> addEntity(String tableAlias, String entityName);

	@Override
	ReactiveNativeQueryImplementor<R> addEntity(String tableAlias, String entityName, LockMode lockMode);

	@Override
	ReactiveNativeQueryImplementor<R> addEntity(@SuppressWarnings("rawtypes") Class entityType);

	@Override
	ReactiveNativeQueryImplementor<R> addEntity(String tableAlias, @SuppressWarnings("rawtypes") Class entityType);

	@Override
	ReactiveNativeQueryImplementor<R> addEntity(String tableAlias, @SuppressWarnings("rawtypes") Class entityClass, LockMode lockMode);

	@Override
	ReactiveNativeQueryImplementor<R> addJoin(String tableAlias, String path);

	@Override
	ReactiveNativeQueryImplementor<R> addJoin(
			String tableAlias,
			String ownerTableAlias,
			String joinPropertyName);

	@Override
	ReactiveNativeQueryImplementor<R> addJoin(String tableAlias, String path, LockMode lockMode);

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// covariant overrides - Query / QueryImplementor

	@Override
	ReactiveNativeQueryImplementor<R> setHint(String hintName, Object value);

	@Override
	ReactiveNativeQueryImplementor<R> setHibernateFlushMode(FlushMode flushMode);

	@Override
	ReactiveNativeQueryImplementor<R> setFlushMode(FlushModeType flushMode);

	@Override
	ReactiveNativeQueryImplementor<R> setCacheMode(CacheMode cacheMode);

	@Override
	ReactiveNativeQueryImplementor<R> setCacheable(boolean cacheable);

	@Override
	ReactiveNativeQueryImplementor<R> setCacheRegion(String cacheRegion);

	@Override
	ReactiveNativeQueryImplementor<R> setTimeout(int timeout);

	@Override
	ReactiveNativeQueryImplementor<R> setFetchSize(int fetchSize);

	@Override
	ReactiveNativeQueryImplementor<R> setReadOnly(boolean readOnly);

	@Override
	ReactiveNativeQueryImplementor<R> setLockOptions(LockOptions lockOptions);

	@Override
	ReactiveNativeQueryImplementor<R> setHibernateLockMode(LockMode lockMode);

	@Override
	ReactiveNativeQueryImplementor<R> setLockMode(LockModeType lockMode);

	@Override
	ReactiveNativeQueryImplementor<R> setLockMode(String alias, LockMode lockMode);

	@Override
	ReactiveNativeQueryImplementor<R> setComment(String comment);

	@Override
	ReactiveNativeQueryImplementor<R> setMaxResults(int maxResult);

	@Override
	ReactiveNativeQueryImplementor<R> setFirstResult(int startPosition);

	@Override
	ReactiveNativeQueryImplementor<R> addQueryHint(String hint);

	@Override
	<T> ReactiveNativeQueryImplementor<T> setTupleTransformer(TupleTransformer<T> transformer);

	@Override
	ReactiveNativeQueryImplementor<R> setResultListTransformer(ResultListTransformer<R> transformer);

	@Override
	ReactiveNativeQueryImplementor<R> setParameter(String name, Object val);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameter(String name, P val, BindableType<P> type);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameter(String name, P val, Class<P> type);

	@Override
	ReactiveNativeQueryImplementor<R> setParameter(String name, Instant value, TemporalType temporalType);

	@Override
	ReactiveNativeQueryImplementor<R> setParameter(String name, Date value, TemporalType temporalType);

	@Override
	ReactiveNativeQueryImplementor<R> setParameter(String name, Calendar value, TemporalType temporalType);

	@Override
	ReactiveNativeQueryImplementor<R> setParameter(int position, Object val);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameter(int position, P val, Class<P> type);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameter(int position, P val, BindableType<P> type);

	@Override
	ReactiveNativeQueryImplementor<R> setParameter(int position, Instant value, TemporalType temporalType);

	@Override
	ReactiveNativeQueryImplementor<R> setParameter(int position, Date value, TemporalType temporalType);

	@Override
	ReactiveNativeQueryImplementor<R> setParameter(int position, Calendar value, TemporalType temporalType);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameter(QueryParameter<P> parameter, P val);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameter(QueryParameter<P> parameter, P val, Class<P> type);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameter(Parameter<P> param, P value);

	@Override
	ReactiveNativeQueryImplementor<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

	@Override
	ReactiveNativeQueryImplementor<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

	@Override
	ReactiveNativeQueryImplementor<R> setParameterList(String name, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(String name, Collection<? extends P> values, Class<P> type);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(String name, Collection<? extends P> values, BindableType<P> type);

	@Override
	ReactiveNativeQueryImplementor<R> setParameterList(String name, Object[] values);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(String name, P[] values, Class<P> type);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(String name, P[] values, BindableType<P> type);

	@Override
	ReactiveNativeQueryImplementor<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(int position, Collection<? extends P> values, Class<P> type);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(int position, Collection<? extends P> values, BindableType<P> type);

	@Override
	ReactiveNativeQueryImplementor<R> setParameterList(int position, Object[] values);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(int position, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(int position, P[] values, BindableType<P> type);


	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, BindableType<P> type);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveNativeQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values, BindableType<P> type);

	@Override
	ReactiveNativeQueryImplementor<R> setProperties(Object bean);

	@Override
	ReactiveNativeQueryImplementor<R> setProperties(@SuppressWarnings("rawtypes") Map bean);
}
