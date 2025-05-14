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

import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.spi.QueryParameterBindings;

import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.Type;

public interface ReactiveQueryImplementor<R> extends ReactiveQuery<R> {

	QueryParameterBindings getParameterBindings();

	@Override
	<T> ReactiveQueryImplementor<T> setTupleTransformer(TupleTransformer<T> transformer);

	@Override
	ReactiveQueryImplementor<R> setResultListTransformer(ResultListTransformer<R> transformer);

	@Override
	ReactiveQueryImplementor<R> setParameter(String name, Object value);

	@Override
	<P> ReactiveQueryImplementor<R> setParameter(String name, P value, Class<P> type);

	@Override
	<P> ReactiveQueryImplementor<R> setParameter(String name, P value, Type<P> type);

	@Override
	ReactiveQueryImplementor<R> setParameter(String name, Instant value, TemporalType temporalType);

	@Override
	ReactiveQueryImplementor<R> setParameter(String name, Calendar value, TemporalType temporalType);

	@Override
	ReactiveQueryImplementor<R> setParameter(String name, Date value, TemporalType temporalType);

	@Override
	ReactiveQueryImplementor<R> setParameter(int position, Object value);

	@Override
	<P> ReactiveQueryImplementor<R> setParameter(int position, P value, Class<P> type);

	@Override
	<P> ReactiveQueryImplementor<R> setParameter(int position, P value, Type<P> type);

	@Override
	ReactiveQueryImplementor<R> setParameter(int position, Instant value, TemporalType temporalType);

	@Override
	ReactiveQueryImplementor<R> setParameter(int position, Date value, TemporalType temporalType);

	@Override
	ReactiveQueryImplementor<R> setParameter(int position, Calendar value, TemporalType temporalType);

	@Override
	<T> ReactiveQueryImplementor<R> setParameter(QueryParameter<T> parameter, T value);

	@Override
	<P> ReactiveQueryImplementor<R> setParameter(QueryParameter<P> parameter, P value, Class<P> type);

	@Override
	<P> ReactiveQueryImplementor<R> setParameter(QueryParameter<P> parameter, P val, Type<P> type);

	@Override
	<T> ReactiveQueryImplementor<R> setParameter(Parameter<T> param, T value);

	@Override
	ReactiveQueryImplementor<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

	@Override
	ReactiveQueryImplementor<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

	@Override
	ReactiveQueryImplementor<R> setParameterList(String name, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(String name, Collection<? extends P> values, Type<P> type);

	@Override
	ReactiveQueryImplementor<R> setParameterList(String name, Object[] values);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(String name, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(String name, P[] values, Type<P> type);

	@Override
	ReactiveQueryImplementor<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(
			int position,
			Collection<? extends P> values,
			Type<P> type);

	@Override
	ReactiveQueryImplementor<R> setParameterList(int position, Object[] values);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(int position, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(int position, P[] values, Type<P> type);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(
			QueryParameter<P> parameter,
			Collection<? extends P> values,
			Class<P> javaType);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(
			QueryParameter<P> parameter,
			Collection<? extends P> values,
			Type<P> type);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values, Type<P> type);

	@Override
	ReactiveQueryImplementor<R> setProperties(Object bean);

	@Override
	ReactiveQueryImplementor<R> setProperties(@SuppressWarnings("rawtypes") Map bean);
}
