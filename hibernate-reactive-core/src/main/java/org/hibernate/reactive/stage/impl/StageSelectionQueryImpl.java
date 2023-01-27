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
import org.hibernate.reactive.query.ReactiveSelectionQuery;
import org.hibernate.reactive.stage.Stage;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;

public class StageSelectionQueryImpl<T> implements Stage.SelectionQuery<T> {
	private final ReactiveSelectionQuery<T> delegate;

	public StageSelectionQueryImpl(ReactiveSelectionQuery<T> delegate) {
		this.delegate = delegate;
	}

	public String getQueryString() {
		return delegate.getQueryString();
	}

	public CompletionStage<List<T>> getReactiveResultList() {
		return delegate.getReactiveResultList();
	}

	public CompletionStage<List<T>> reactiveList() {
		return delegate.reactiveList();
	}

	public CompletionStage<T> getReactiveSingleResult() {
		return delegate.getReactiveSingleResult();
	}

	public CompletionStage<T> getReactiveSingleResultOrNull() {
		return delegate.getReactiveSingleResultOrNull();
	}

	public CompletionStage<T> reactiveUnique() {
		return delegate.reactiveUnique();
	}

	public CompletionStage<Optional<T>> reactiveUniqueResultOptional() {
		return delegate.reactiveUniqueResultOptional();
	}

	@Override
	public CompletionStage<List<T>> list() {
		return null;
	}

	@Override
	public CompletionStage<T> getSingleResult() {
		return null;
	}

	@Override
	public CompletionStage<T> getSingleResultOrNull() {
		return null;
	}

	@Override
	public CompletionStage<T> uniqueResult() {
		return delegate.reactiveUnique();
	}

	@Override
	public CompletionStage<Optional<T>> uniqueResultOptional() {
		return delegate.reactiveUniqueResultOptional();
	}

	@Override
	public Stage.SelectionQuery<T> setHint(String hintName, Object value) {
		delegate.setHint( hintName, value );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setFlushMode(FlushModeType flushMode) {
		delegate.setFlushMode( flushMode );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setHibernateFlushMode(FlushMode flushMode) {
		delegate.setHibernateFlushMode( flushMode );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setTimeout(int timeout) {
		delegate.setTimeout( timeout );
		return this;
	}

	@Override
	public Integer getFetchSize() {
		return delegate.getFetchSize();
	}

	@Override
	public Stage.SelectionQuery<T> setFetchSize(int fetchSize) {
		delegate.setFetchSize( fetchSize );
		return this;
	}

	@Override
	public boolean isReadOnly() {
		return delegate.isReadOnly();
	}

	@Override
	public Stage.SelectionQuery<T> setReadOnly(boolean readOnly) {
		delegate.setReadOnly( readOnly );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setMaxResults(int maxResult) {
		delegate.setMaxResults( maxResult );
		return this;
	}

	@Override
	public int getFirstResult() {
		return delegate.getFirstResult();
	}

	@Override
	public Stage.SelectionQuery<T> setFirstResult(int startPosition) {
		delegate.setFirstResult( startPosition );
		return this;
	}

	@Override
	public CacheMode getCacheMode() {
		return delegate.getCacheMode();
	}

	@Override
	public CacheStoreMode getCacheStoreMode() {
		return delegate.getCacheStoreMode();
	}

	@Override
	public CacheRetrieveMode getCacheRetrieveMode() {
		return delegate.getCacheRetrieveMode();
	}

	@Override
	public Stage.SelectionQuery<T> setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		delegate.setCacheStoreMode( cacheStoreMode );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		delegate.setCacheRetrieveMode( cacheRetrieveMode );
		return this;
	}

	@Override
	public boolean isCacheable() {
		return delegate.isCacheable();
	}

	@Override
	public Stage.SelectionQuery<T> setCacheable(boolean cacheable) {
		delegate.setCacheable( cacheable );
		return this;
	}

	@Override
	public String getCacheRegion() {
		return delegate.getCacheRegion();
	}

	@Override
	public Stage.SelectionQuery<T> setCacheRegion(String cacheRegion) {
		delegate.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public LockOptions getLockOptions() {
		return delegate.getLockOptions();
	}

	@Override
	public LockModeType getLockMode() {
		return delegate.getLockMode();
	}

	@Override
	public Stage.SelectionQuery<T> setLockMode(LockModeType lockMode) {
		delegate.setLockMode( lockMode );
		return this;
	}

	@Override
	public LockMode getHibernateLockMode() {
		return delegate.getHibernateLockMode();
	}

	@Override
	public Stage.SelectionQuery<T> setHibernateLockMode(LockMode lockMode) {
		delegate.setHibernateLockMode( lockMode );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setLockMode(String alias, LockMode lockMode) {
		delegate.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setAliasSpecificLockMode(String alias, LockMode lockMode) {
		delegate.setAliasSpecificLockMode( alias, lockMode );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setFollowOnLocking(boolean enable) {
		delegate.setFollowOnLocking( enable );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameter(String name, Object value) {
		delegate.setParameter( name, value );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameter(String name, P value, Class<P> type) {
		delegate.setParameter( name, value, type );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameter(String name, P value, BindableType<P> type) {
		delegate.setParameter( name, value, type );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameter(String name, Instant value, TemporalType temporalType) {
		delegate.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameter(String name, Calendar value, TemporalType temporalType) {
		delegate.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameter(String name, Date value, TemporalType temporalType) {
		delegate.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameter(int position, Object value) {
		delegate.setParameter( position, value );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameter(int position, P value, Class<P> type) {
		delegate.setParameter( position, value, type );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameter(int position, P value, BindableType<P> type) {
		delegate.setParameter( position, value, type );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameter(int position, Instant value, TemporalType temporalType) {
		delegate.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameter(int position, Date value, TemporalType temporalType) {
		delegate.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameter(int position, Calendar value, TemporalType temporalType) {
		delegate.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public <T1> Stage.SelectionQuery<T> setParameter(QueryParameter<T1> parameter, T1 value) {
		delegate.setParameter( parameter, value );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameter(QueryParameter<P> parameter, P value, Class<P> type) {
		delegate.setParameter( parameter, value, type );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type) {
		delegate.setParameter( parameter, val, type );
		return this;
	}

	@Override
	public <T1> Stage.SelectionQuery<T> setParameter(Parameter<T1> param, T1 value) {
		delegate.setParameter( param, value );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameter(
			Parameter<Calendar> param,
			Calendar value,
			TemporalType temporalType) {
		delegate.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameter(Parameter<Date> param, Date value, TemporalType temporalType) {
		delegate.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameterList(String name, Collection values) {
		delegate.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(
			String name,
			Collection<? extends P> values,
			Class<P> javaType) {
		delegate.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(
			String name,
			Collection<? extends P> values,
			BindableType<P> type) {
		delegate.setParameterList( name, values, type );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameterList(String name, Object[] values) {
		delegate.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(String name, P[] values, Class<P> javaType) {
		delegate.setParameterList( name, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(String name, P[] values, BindableType<P> type) {
		delegate.setParameterList( name, values, type );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameterList(int position, Collection values) {
		delegate.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(
			int position,
			Collection<? extends P> values,
			Class<P> javaType) {
		delegate.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(
			int position,
			Collection<? extends P> values,
			BindableType<P> type) {
		delegate.setParameterList( position, values, type );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setParameterList(int position, Object[] values) {
		delegate.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(int position, P[] values, Class<P> javaType) {
		delegate.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(int position, P[] values, BindableType<P> type) {
		delegate.setParameterList( position, values, type );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values) {
		delegate.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(
			QueryParameter<P> parameter,
			Collection<? extends P> values,
			Class<P> javaType) {
		delegate.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(
			QueryParameter<P> parameter,
			Collection<? extends P> values,
			BindableType<P> type) {
		delegate.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(QueryParameter<P> parameter, P[] values) {
		delegate.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType) {
		delegate.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> Stage.SelectionQuery<T> setParameterList(
			QueryParameter<P> parameter,
			P[] values,
			BindableType<P> type) {
		delegate.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setProperties(Object bean) {
		delegate.setProperties( bean );
		return this;
	}

	@Override
	public Stage.SelectionQuery<T> setProperties(Map bean) {
		delegate.setProperties( bean );
		return this;
	}

	@Override
	public FlushModeType getFlushMode() {
		return delegate.getFlushMode();
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
	public String getComment() {
		return delegate.getComment();
	}

	@Override
	public CommonQueryContract setComment(String comment) {
		return delegate.setComment( comment );
	}
}
