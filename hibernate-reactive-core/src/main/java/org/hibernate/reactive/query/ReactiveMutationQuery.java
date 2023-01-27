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
import java.util.concurrent.CompletionStage;

import org.hibernate.FlushMode;
import org.hibernate.query.BindableType;
import org.hibernate.query.CommonQueryContract;
import org.hibernate.query.QueryParameter;

import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;

/**
 * @see org.hibernate.query.MutationQuery
 */
public interface ReactiveMutationQuery<R> extends CommonQueryContract {
	CompletionStage<Integer> executeReactiveUpdate();

	@Override
	ReactiveMutationQuery<R> setParameter(String name, Object value);

	@Override
	<P> ReactiveMutationQuery<R> setParameter(String name, P value, Class<P> type);

	@Override
	<P> ReactiveMutationQuery<R> setParameter(String name, P value, BindableType<P> type);

	@Override
	ReactiveMutationQuery<R> setParameter(String name, Instant value, TemporalType temporalType);

	@Override
	ReactiveMutationQuery<R> setParameter(String name, Calendar value, TemporalType temporalType);

	@Override
	ReactiveMutationQuery<R> setParameter(String name, Date value, TemporalType temporalType);

	@Override
	ReactiveMutationQuery<R> setParameter(int position, Object value);

	@Override
	<P> ReactiveMutationQuery<R> setParameter(int position, P value, Class<P> type);

	@Override
	<P> ReactiveMutationQuery<R> setParameter(int position, P value, BindableType<P> type);

	@Override
	ReactiveMutationQuery<R> setParameter(int position, Instant value, TemporalType temporalType);

	@Override
	ReactiveMutationQuery<R> setParameter(int position, Date value, TemporalType temporalType);

	@Override
	ReactiveMutationQuery<R> setParameter(int position, Calendar value, TemporalType temporalType);

	@Override
	<T> ReactiveMutationQuery<R> setParameter(QueryParameter<T> parameter, T value);

	@Override
	<P> ReactiveMutationQuery<R> setParameter(QueryParameter<P> parameter, P value, Class<P> type);

	@Override
	<P> ReactiveMutationQuery<R> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type);

	@Override
	<T> ReactiveMutationQuery<R> setParameter(Parameter<T> param, T value);

	@Override
	ReactiveMutationQuery<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

	@Override
	ReactiveMutationQuery<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

	@Override
	ReactiveMutationQuery<R> setParameterList(String name, Collection values);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(String name, Collection<? extends P> values, BindableType<P> type);

	@Override
	ReactiveMutationQuery<R> setParameterList(String name, Object[] values);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(String name, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(String name, P[] values, BindableType<P> type);

	@Override
	ReactiveMutationQuery<R> setParameterList(int position, Collection values);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(int position, Collection<? extends P> values, BindableType<P> type);

	@Override
	ReactiveMutationQuery<R> setParameterList(int position, Object[] values);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(int position, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(int position, P[] values, BindableType<P> type);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, BindableType<P> type);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(QueryParameter<P> parameter, P[] values);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveMutationQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, BindableType<P> type);

	@Override
	ReactiveMutationQuery<R> setProperties(Object bean);

	@Override
	ReactiveMutationQuery<R> setProperties(Map bean);

	@Override
	ReactiveMutationQuery<R> setHibernateFlushMode(FlushMode flushMode);
}
