/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sqm;

import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.spi.SqmQuery;
import org.hibernate.reactive.query.ReactiveSelectionQuery;

import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.Type;

/**
 * @see org.hibernate.query.sqm.SqmSelectionQuery
 */
public interface ReactiveSqmSelectionQuery<R> extends ReactiveSelectionQuery<R>, SqmQuery<R> {

	@Override
	ReactiveSqmSelectionQuery<R> setParameter(String name, Object value);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameter(String name, P value, Class<P> type);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameter(String name, P value, Type<P> type);

	@Override
	ReactiveSqmSelectionQuery<R> setParameter(String name, Instant value, TemporalType temporalType);

	@Override
	ReactiveSqmSelectionQuery<R> setParameter(String name, Calendar value, TemporalType temporalType);

	@Override
	ReactiveSqmSelectionQuery<R> setParameter(String name, Date value, TemporalType temporalType);

	@Override
	ReactiveSqmSelectionQuery<R> setParameter(int position, Object value);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameter(int position, P value, Class<P> type);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameter(int position, P value, Type<P> type);

	@Override
	ReactiveSqmSelectionQuery<R> setParameter(int position, Instant value, TemporalType temporalType);

	@Override
	ReactiveSqmSelectionQuery<R> setParameter(int position, Date value, TemporalType temporalType);

	@Override
	ReactiveSqmSelectionQuery<R> setParameter(int position, Calendar value, TemporalType temporalType);

	@Override
	<T> ReactiveSqmSelectionQuery<R> setParameter(QueryParameter<T> parameter, T value);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameter(QueryParameter<P> parameter, P value, Class<P> type);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameter(QueryParameter<P> parameter, P val, Type<P> type);

	@Override
	<T> ReactiveSqmSelectionQuery<R> setParameter(Parameter<T> param, T value);

	@Override
	ReactiveSqmSelectionQuery<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

	@Override
	ReactiveSqmSelectionQuery<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

	@Override
	ReactiveSqmSelectionQuery<R> setParameterList(String name, Collection values);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(
			String name,
			Collection<? extends P> values,
			Type<P> type);

	@Override
	ReactiveSqmSelectionQuery<R> setParameterList(String name, Object[] values);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(String name, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(String name, P[] values, Type<P> type);

	@Override
	ReactiveSqmSelectionQuery<R> setParameterList(int position, Collection values);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(
			int position,
			Collection<? extends P> values,
			Type<P> type);

	@Override
	ReactiveSqmSelectionQuery<R> setParameterList(int position, Object[] values);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(int position, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(int position, P[] values, Type<P> type);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(
			QueryParameter<P> parameter,
			Collection<? extends P> values,
			Class<P> javaType);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(
			QueryParameter<P> parameter,
			Collection<? extends P> values,
			Type<P> type);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(QueryParameter<P> parameter, P[] values);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSqmSelectionQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, Type<P> type);

	@Override
	ReactiveSqmSelectionQuery<R> setProperties(Object bean);

	@Override
	ReactiveSqmSelectionQuery<R> setProperties(Map bean);

	@Override
	ReactiveSqmSelectionQuery<R> setHibernateFlushMode(FlushMode flushMode);

	@Override
	ReactiveSqmSelectionQuery<R> setCacheMode(CacheMode cacheMode);

	@Override
	ReactiveSqmSelectionQuery<R> setCacheable(boolean cacheable);

	@Override
	ReactiveSqmSelectionQuery<R> setCacheRegion(String cacheRegion);

	@Override
	ReactiveSqmSelectionQuery<R> setTimeout(int timeout);

	@Override
	ReactiveSqmSelectionQuery<R> setFetchSize(int fetchSize);

	@Override
	ReactiveSqmSelectionQuery<R> setReadOnly(boolean readOnly);
}
