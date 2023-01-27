/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

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
import org.hibernate.reactive.query.ReactiveMutationQuery;
import org.hibernate.reactive.stage.Stage;

import jakarta.persistence.FlushModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;

public class StageMutationQueryImpl<T> implements Stage.MutationQuery<T> {

	private final ReactiveMutationQuery<T> delegate;

	public StageMutationQueryImpl(ReactiveMutationQuery<T> delegate) {
		this.delegate = delegate;
	}

	public CompletionStage<Integer> executeReactiveUpdate() {
		return delegate.executeReactiveUpdate();
	}

	@Override
	public CompletionStage<Integer> executeUpdate() {
		return delegate.executeReactiveUpdate();
	}

	@Override
	public Stage.MutationQuery<T> setParameter(String name, Object value) {
		delegate.setParameter( name, value );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameter(String name, P value, Class<P> type) {
		delegate.setParameter( name, value, type );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameter(String name, P value, BindableType<P> type) {
		delegate.setParameter( name, value, type );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameter(String name, Instant value, TemporalType temporalType) {
		delegate.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameter(String name, Calendar value, TemporalType temporalType) {
		delegate.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameter(String name, Date value, TemporalType temporalType) {
		delegate.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameter(int position, Object value) {
		delegate.setParameter( position, value );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameter(int position, P value, Class<P> type) {
		delegate.setParameter( position, value, type );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameter(int position, P value, BindableType<P> type) {
		delegate.setParameter( position, value, type );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameter(int position, Instant value, TemporalType temporalType) {
		delegate.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameter(int position, Date value, TemporalType temporalType) {
		delegate.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameter(int position, Calendar value, TemporalType temporalType) {
		delegate.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public <T1> Stage.MutationQuery<T> setParameter(QueryParameter<T1> parameter, T1 value) {
		delegate.setParameter( parameter, value );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameter(QueryParameter<P> parameter, P value, Class<P> type) {
		delegate.setParameter( parameter, value, type );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type) {
		delegate.setParameter( parameter, val, type );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameter(Parameter<P> param, P value) {
		delegate.setParameter( param, value );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType) {
		delegate.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameter(Parameter<Date> param, Date value, TemporalType temporalType) {
		delegate.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameterList(String name, Collection values) {
		delegate.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(
			String name,
			Collection<? extends P> values,
			Class<P> javaType) {
		delegate.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(
			String name,
			Collection<? extends P> values,
			BindableType<P> type) {
		delegate.setParameterList( name, values, type );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameterList(String name, Object[] values) {
		delegate.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(String name, P[] values, Class<P> javaType) {
		delegate.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(String name, P[] values, BindableType<P> type) {
		delegate.setParameterList( name, values, type );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameterList(int position, Collection values) {
		delegate.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(
			int position,
			Collection<? extends P> values,
			Class<P> javaType) {
		delegate.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(
			int position,
			Collection<? extends P> values,
			BindableType<P> type) {
		delegate.setParameterList( position, values, type );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setParameterList(int position, Object[] values) {
		delegate.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(int position, P[] values, Class<P> javaType) {
		delegate.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(int position, P[] values, BindableType<P> type) {
		delegate.setParameterList( position, values, type );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values) {
		delegate.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(
			QueryParameter<P> parameter,
			Collection<? extends P> values,
			Class<P> javaType) {
		delegate.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(
			QueryParameter<P> parameter,
			Collection<? extends P> values,
			BindableType<P> type) {
		delegate.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(QueryParameter<P> parameter, P[] values) {
		delegate.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType) {
		delegate.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.MutationQuery<T> setParameterList(
			QueryParameter<P> parameter,
			P[] values,
			BindableType<P> type) {
		delegate.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setProperties(Object bean) {
		delegate.setProperties( bean );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setProperties(Map bean) {
		delegate.setProperties( bean );
		return this;
	}

	@Override
	public Stage.MutationQuery<T> setHibernateFlushMode(FlushMode flushMode) {
		delegate.setHibernateFlushMode( flushMode );
		return this;
	}

	@Override
	public FlushModeType getFlushMode() {
		return delegate.getFlushMode();
	}

	@Override
	public CommonQueryContract setFlushMode(FlushModeType flushMode) {
		return delegate.setFlushMode( flushMode );
	}

	@Override
	public FlushMode getHibernateFlushMode() {
		return delegate.getHibernateFlushMode();
	}

	@Override
	public Integer getTimeout() {
		return delegate.getTimeout();
	}

	@Override
	public CommonQueryContract setTimeout(int timeout) {
		return delegate.setTimeout( timeout );
	}

	@Override
	public String getComment() {
		return delegate.getComment();
	}

	@Override
	public CommonQueryContract setComment(String comment) {
		return delegate.setComment( comment );
	}

	@Override
	public CommonQueryContract setHint(String hintName, Object value) {
		return delegate.setHint( hintName, value );
	}
}
