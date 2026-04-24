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

import org.hibernate.FlushMode;
import org.hibernate.query.QueryParameter;
import org.hibernate.reactive.query.ReactiveMutationQuery;
import org.hibernate.reactive.query.ReactiveQueryImplementor;

import jakarta.persistence.FlushModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.Type;

/**
 * An internal contract for a reactive mutation query.
 * Mirrors {@link org.hibernate.query.spi.MutationQueryImplementor}.
 *
 * @param <R> the result type
 */
public interface ReactiveMutationQueryImplementor<R> extends ReactiveQueryImplementor<R>, ReactiveMutationQuery<R> {

	@Override
	ReactiveMutationQueryImplementor<R> setHibernateFlushMode(FlushMode flushMode);

	@Override
	ReactiveMutationQueryImplementor<R> setFlushMode(FlushModeType flushMode);

	@Override
	ReactiveMutationQueryImplementor<R> setTimeout(int timeout);

	@Override
	ReactiveMutationQueryImplementor<R> setHint(String hintName, Object value);

	@Override
	ReactiveMutationQueryImplementor<R> setParameter(String name, Object value);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameter(String name, P value, Class<P> type);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameter(String name, P value, Type<P> type);

	@Override
	ReactiveMutationQueryImplementor<R> setParameter(String name, Instant value, TemporalType temporalType);

	@Override
	ReactiveMutationQueryImplementor<R> setParameter(String name, Calendar value, TemporalType temporalType);

	@Override
	ReactiveMutationQueryImplementor<R> setParameter(String name, Date value, TemporalType temporalType);

	@Override
	ReactiveMutationQueryImplementor<R> setParameter(int position, Object value);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameter(int position, P value, Class<P> type);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameter(int position, P value, Type<P> type);

	@Override
	ReactiveMutationQueryImplementor<R> setParameter(int position, Instant value, TemporalType temporalType);

	@Override
	ReactiveMutationQueryImplementor<R> setParameter(int position, Calendar value, TemporalType temporalType);

	@Override
	ReactiveMutationQueryImplementor<R> setParameter(int position, Date value, TemporalType temporalType);

	@Override
	<T> ReactiveMutationQueryImplementor<R> setParameter(QueryParameter<T> parameter, T value);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameter(QueryParameter<P> parameter, P value, Class<P> type);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameter(QueryParameter<P> parameter, P val, Type<P> type);

	@Override
	<T> ReactiveMutationQueryImplementor<R> setParameter(Parameter<T> param, T value);

	@Override
	ReactiveMutationQueryImplementor<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

	@Override
	ReactiveMutationQueryImplementor<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

	@Override
	ReactiveMutationQueryImplementor<R> setParameterList(String name, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(String name, Collection<? extends P> values, Type<P> type);

	@Override
	ReactiveMutationQueryImplementor<R> setParameterList(String name, Object[] values);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(String name, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(String name, P[] values, Type<P> type);

	@Override
	ReactiveMutationQueryImplementor<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(int position, Collection<? extends P> values, Type<P> type);

	@Override
	ReactiveMutationQueryImplementor<R> setParameterList(int position, Object[] values);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(int position, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(int position, P[] values, Type<P> type);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Type<P> type);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values, Type<P> type);

	@Override
	ReactiveMutationQueryImplementor<R> setProperties(Object bean);

	@Override
	ReactiveMutationQueryImplementor<R> setProperties(@SuppressWarnings("rawtypes") Map bean);
}
