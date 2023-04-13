/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.query.BindableType;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.named.NameableQuery;
import org.hibernate.query.sqm.tree.SqmStatement;
import org.hibernate.reactive.query.ReactiveQueryImplementor;

import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;


/**
 * An internal contract between the reactive session implementation
 * and the {@link org.hibernate.reactive.stage.Stage.Query} and
 * {@link org.hibernate.reactive.mutiny.Mutiny.Query} APIs.
 *
 * @see ReactiveSession
 */
@Incubating
public interface ReactiveSqmQueryImplementor<R> extends ReactiveQueryImplementor<R>, NameableQuery {

	SqmStatement<R> getSqmStatement();

	// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	// covariance

	ReactiveSqmQueryImplementor<R> setCacheMode(CacheMode cacheMode);

	ReactiveSqmQueryImplementor<R> setCacheable(boolean cacheable);

	ReactiveSqmQueryImplementor<R> setCacheRegion(String cacheRegion);

	ReactiveSqmQueryImplementor<R> setTimeout(int timeout);

	ReactiveSqmQueryImplementor<R> setFetchSize(int fetchSize);

	ReactiveSqmQueryImplementor<R> setReadOnly(boolean readOnly);

	@Override
	ReactiveSqmQueryImplementor<R> applyGraph(RootGraph<?> graph, GraphSemantic semantic);

	@Override
	default ReactiveSqmQueryImplementor<R> applyFetchGraph(RootGraph<?> graph) {
		ReactiveQueryImplementor.super.applyFetchGraph( graph );
		return this;
	}

	@Override
	default ReactiveSqmQueryImplementor<R> applyLoadGraph(RootGraph<?> graph) {
		ReactiveQueryImplementor.super.applyLoadGraph( graph );
		return this;
	}

	@Override
	ReactiveSqmQueryImplementor<R> setComment(String comment);

	@Override
	ReactiveSqmQueryImplementor<R> addQueryHint(String hint);

	@Override
	ReactiveSqmQueryImplementor<R> setLockOptions(LockOptions lockOptions);

	@Override
	ReactiveSqmQueryImplementor<R> setLockMode(String alias, LockMode lockMode);

	@Override
	<T> ReactiveSqmQueryImplementor<T> setTupleTransformer(TupleTransformer<T> transformer);

	@Override
	ReactiveSqmQueryImplementor<R> setResultListTransformer(ResultListTransformer transformer);


	@Override
	ReactiveSqmQueryImplementor<R> setHibernateFlushMode(FlushMode flushMode);

	@Override
	ReactiveSqmQueryImplementor<R> setMaxResults(int maxResult);

	@Override
	ReactiveSqmQueryImplementor<R> setFirstResult(int startPosition);

	@Override
	ReactiveSqmQueryImplementor<R> setHint(String hintName, Object value);

	@Override
	ReactiveSqmQueryImplementor<R> setFlushMode(FlushModeType flushMode);

	@Override
	ReactiveSqmQueryImplementor<R> setLockMode(LockModeType lockMode);

	@Override
	ReactiveSqmQueryImplementor<R> setParameter(String name, Object value);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameter(String name, P value, Class<P> type);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameter(String name, P value, BindableType<P> type);

	@Override
	ReactiveSqmQueryImplementor<R> setParameter(String name, Instant value, TemporalType temporalType);

	@Override
	ReactiveSqmQueryImplementor<R> setParameter(String name, Calendar value, TemporalType temporalType);

	@Override
	ReactiveSqmQueryImplementor<R> setParameter(String name, Date value, TemporalType temporalType);

	@Override
	ReactiveSqmQueryImplementor<R> setParameter(int position, Object value);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameter(int position, P value, Class<P> type);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameter(int position, P value, BindableType<P> type);

	@Override
	ReactiveSqmQueryImplementor<R> setParameter(int position, Instant value, TemporalType temporalType);

	@Override
	ReactiveSqmQueryImplementor<R> setParameter(int position, Date value, TemporalType temporalType);

	@Override
	ReactiveSqmQueryImplementor<R> setParameter(int position, Calendar value, TemporalType temporalType);

	@Override
	<T> ReactiveSqmQueryImplementor<R> setParameter(QueryParameter<T> parameter, T value);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameter(QueryParameter<P> parameter, P value, Class<P> type);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type);

	@Override
	<T> ReactiveSqmQueryImplementor<R> setParameter(Parameter<T> param, T value);

	@Override
	ReactiveSqmQueryImplementor<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

	@Override
	ReactiveSqmQueryImplementor<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

	@Override
	ReactiveSqmQueryImplementor<R> setParameterList(String name, Collection values);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(String name, Collection<? extends P> values, BindableType<P> type);

	@Override
	ReactiveSqmQueryImplementor<R> setParameterList(String name, Object[] values);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(String name, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(String name, P[] values, BindableType<P> type);

	@Override
	ReactiveSqmQueryImplementor<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(int position, Collection<? extends P> values, BindableType<P> type);

	@Override
	ReactiveSqmQueryImplementor<R> setParameterList(int position, Object[] values);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(int position, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(int position, P[] values, BindableType<P> type);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, BindableType<P> type);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

	@Override
	<P> ReactiveSqmQueryImplementor<R> setParameterList(QueryParameter<P> parameter, P[] values, BindableType<P> type);

	@Override
	ReactiveSqmQueryImplementor<R> setProperties(Object bean);

	@Override
	ReactiveSqmQueryImplementor<R> setProperties(Map bean);
}
