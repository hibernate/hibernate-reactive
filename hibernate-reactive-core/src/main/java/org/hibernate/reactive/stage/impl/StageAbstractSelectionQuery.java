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
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.BindableType;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.spi.AbstractCommonQueryContract;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.ParameterMetadataImplementor;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.sql.exec.spi.Callback;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;

public abstract class StageAbstractSelectionQuery<R> extends AbstractCommonQueryContract
		implements Stage.SelectionQuery<R>, DomainQueryExecutionContext {

	public StageAbstractSelectionQuery(SharedSessionContractImplementor session) {
		super( session );
	}

	@Override
	public Stage.SelectionQuery<R> setHint(String hintName, Object value) {
		return null;
	}

	@Override
	public Stage.SelectionQuery setProperties(Map map) {
		return null;
	}

	@Override
	public Stage.SelectionQuery setProperties(Object bean) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setHibernateFlushMode(FlushMode flushMode) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setTimeout(int timeout) {
		return null;
	}

	@Override
	protected ParameterMetadataImplementor getParameterMetadata() {
		return null;
	}

	@Override
	public QueryParameterBindings getQueryParameterBindings() {
		return null;
	}

	@Override
	public Callback getCallback() {
		return null;
	}

	@Override
	protected boolean resolveJdbcParameterTypeIfNecessary() {
		return false;
	}

	@Override
	public CompletionStage<List<R>> list() {
		return null;
	}

	@Override
	public CompletionStage<R> uniqueResult() {
		return null;
	}

	@Override
	public CompletionStage<Optional<R>> uniqueResultOptional() {
		return null;
	}

	@Override
	public FlushModeType getFlushMode() {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setFlushMode(FlushModeType flushMode) {
		return null;
	}

	@Override
	public Integer getFetchSize() {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setFetchSize(int fetchSize) {
		return null;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public Stage.SelectionQuery<R> setReadOnly(boolean readOnly) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setMaxResults(int maxResult) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setFirstResult(int startPosition) {
		return null;
	}

	@Override
	public CacheMode getCacheMode() {
		return null;
	}

	@Override
	public CacheStoreMode getCacheStoreMode() {
		return null;
	}

	@Override
	public CacheRetrieveMode getCacheRetrieveMode() {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setCacheMode(CacheMode cacheMode) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		return null;
	}

	@Override
	public boolean isCacheable() {
		return false;
	}

	@Override
	public Stage.SelectionQuery<R> setCacheable(boolean cacheable) {
		return null;
	}

	@Override
	public String getCacheRegion() {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setCacheRegion(String cacheRegion) {
		return null;
	}

	@Override
	public LockOptions getLockOptions() {
		return null;
	}

	@Override
	public LockModeType getLockMode() {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setLockMode(LockModeType lockMode) {
		return null;
	}

	@Override
	public LockMode getHibernateLockMode() {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setHibernateLockMode(LockMode lockMode) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setLockMode(String alias, LockMode lockMode) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setAliasSpecificLockMode(String alias, LockMode lockMode) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setFollowOnLocking(boolean enable) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(QueryParameter parameter, Object[] values, BindableType type) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(QueryParameter parameter, Object[] values, Class javaType) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(QueryParameter parameter, Object[] values) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(QueryParameter parameter, Collection values, BindableType type) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(QueryParameter parameter, Collection values, Class javaType) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(QueryParameter parameter, Collection values) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(int position, Object[] values) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(int position, Object[] values, BindableType type) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(int position, Object[] values, Class javaType) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(int position, Collection values) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(int position, Collection values, BindableType type) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(int position, Collection values, Class javaType) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(String name, Object[] values, BindableType type) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(String name, Object[] values, Class javaType) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(String name, Collection values, BindableType type) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(String name, Collection values, Class javaType) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(String name, Collection values) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameterList(String name, Object[] values) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameter(String name, Object value) {
		return null;
	}

	@Override
	public <P> Stage.SelectionQuery<R> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type) {
		return null;
	}

	@Override
	public <P> Stage.SelectionQuery<R> setParameter(QueryParameter<P>  parameter, P value, Class<P> type) {
		return null;
	}

	@Override
	public <P> Stage.SelectionQuery<R> setParameter(QueryParameter<P>  parameter, P value) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameter(int position, Object value) {
		return null;
	}

	@Override
	public <P> Stage.SelectionQuery<R> setParameter(int position, P value, Class<P> type) {
		return null;
	}

	@Override
	public <P> Stage.SelectionQuery<R> setParameter(int position, P value, BindableType<P> type) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameter(int position, Date value, TemporalType temporalType) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameter(int position, Calendar value, TemporalType temporalType) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameter(int position, Instant value, TemporalType temporalType) {
		return null;
	}

	@Override
	public <P> Stage.SelectionQuery<R> setParameter(String name, P value, BindableType<P> type) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameter(String name, Instant value, TemporalType temporalType) {
		return null;
	}

	@Override
	public <P> Stage.SelectionQuery<R> setParameter(String name, P value, Class<P> type) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameter(String name, Date value, TemporalType temporalType) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameter(String name, Calendar value, TemporalType temporalType) {
		return null;
	}

	@Override
	public <P> Stage.SelectionQuery<R> setParameter(Parameter<P> param, P value) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType) {
		return null;
	}

	@Override
	public Stage.SelectionQuery<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType) {
		return null;
	}
}
