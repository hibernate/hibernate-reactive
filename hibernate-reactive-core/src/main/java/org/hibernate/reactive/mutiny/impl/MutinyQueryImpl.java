/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.impl;

import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.jpa.internal.util.FlushModeTypeHelper;
import org.hibernate.query.BindableType;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.query.ReactiveQuery;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;

/**
 * Implementation of {@link Mutiny.Query}.
 */
public class MutinyQueryImpl<R> implements Mutiny.Query<R> {

	private final MutinySessionFactoryImpl factory;
	private final ReactiveQuery<R> delegate;

	public MutinyQueryImpl(ReactiveQuery<R> delegate, MutinySessionFactoryImpl factory) {
		this.delegate = delegate;
		this.factory = factory;
	}

	private <T> Uni<T> uni(Supplier<CompletionStage<T>> stageSupplier) {
		return factory.uni( stageSupplier );
	}

	@Override
	public Uni<R> getSingleResult() {
		return uni( delegate::getReactiveSingleResult );
	}

	@Override
	public Uni<R> getSingleResultOrNull() {
		return uni( delegate::getReactiveSingleResultOrNull );
	}

	@Override
	public Uni<List<R>> list() {
		return uni( delegate::reactiveList );
	}

	@Override
	public Uni<R> uniqueResult() {
		return uni( delegate::reactiveUnique );
	}

	@Override
	public Uni<Optional<R>> uniqueResultOptional() {
		return uni( delegate::reactiveUniqueResultOptional );
	}

	@Override
	public Uni<Integer> executeUpdate() {
		return uni( delegate::executeReactiveUpdate );
	}

	@Override
	public Integer getFetchSize() {
		return delegate.getFetchSize();
	}

	@Override
	public boolean isReadOnly() {
		return delegate.isReadOnly();
	}

	@Override
	public int getFirstResult() {
		return delegate.getFirstResult();
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
	public boolean isCacheable() {
		return delegate.isCacheable();
	}

	@Override
	public String getCacheRegion() {
		return delegate.getCacheRegion();
	}

	@Override
	public LockModeType getLockMode() {
		return delegate.getLockMode();
	}

	@Override
	public LockMode getHibernateLockMode() {
		return delegate.getHibernateLockMode();
	}

	@Override
	public Mutiny.SelectionQuery<R> setHibernateLockMode(LockMode lockMode) {
		delegate.setHibernateLockMode( lockMode );
		return this;
	}

	@Override
	public Mutiny.SelectionQuery<R> setAliasSpecificLockMode(String alias, LockMode lockMode) {
		delegate.setAliasSpecificLockMode( alias, lockMode );
		return this;
	}

	@Override
	public Mutiny.SelectionQuery<R> setFollowOnLocking(boolean enable) {
		delegate.setFollowOnLocking( enable );
		return this;
	}

	@Override
	public String getQueryString() {
		return delegate.getQueryString();
	}

	@Override
	public Mutiny.Query<R> applyGraph(RootGraph graph, GraphSemantic semantic) {
		delegate.applyGraph( graph, semantic );
		return this;
	}

	@Override
	public String getComment() {
		return delegate.getComment();
	}

	@Override
	public Mutiny.Query<R> setComment(String comment) {
		delegate.setComment( comment );
		return this;
	}

	@Override
	public Mutiny.Query<R> addQueryHint(String hint) {
		delegate.addQueryHint( hint );
		return this;
	}

	@Override
	public LockOptions getLockOptions() {
		return delegate.getLockOptions();
	}

	@Override
	public Mutiny.Query<R> setLockOptions(LockOptions lockOptions) {
		delegate.setLockOptions( lockOptions );
		return this;
	}

	@Override
	public Mutiny.Query<R> setLockMode(String alias, LockMode lockMode) {
		delegate.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public <T> Mutiny.Query<T> setTupleTransformer(TupleTransformer<T> transformer) {
		return new MutinyQueryImpl<>( delegate.setTupleTransformer( transformer ), factory );
	}

	@Override
	public Mutiny.Query<R> setResultListTransformer(ResultListTransformer<R> transformer) {
		delegate.setResultListTransformer( transformer );
		return this;
	}

	@Override
	public QueryOptions getQueryOptions() {
		return delegate.getQueryOptions();
	}

	@Override
	public ParameterMetadata getParameterMetadata() {
		return delegate.getParameterMetadata();
	}

	@Override
	public Mutiny.Query<R> setParameter(String parameter, Object argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameter(String parameter, P argument, Class<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameter(String parameter, P argument, BindableType<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameter(String parameter, Instant argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameter(String parameter, Calendar argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameter(String parameter, Date argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameter(int parameter, Object argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameter(int parameter, P argument, Class<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameter(int parameter, P argument, BindableType<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameter(int parameter, Instant argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameter(int parameter, Date argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameter(int parameter, Calendar argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public <T> Mutiny.Query<R> setParameter(QueryParameter<T> parameter, T argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameter(QueryParameter<P> parameter, P argument, Class<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameter(QueryParameter<P> parameter, P argument, BindableType<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <T> Mutiny.Query<R> setParameter(Parameter<T> parameter, T argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameter(Parameter<Calendar> parameter, Calendar argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameter(Parameter<Date> parameter, Date argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameterList(String parameter, Collection arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(String parameter, Collection<? extends P> arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(String parameter, Collection<? extends P> arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameterList(String parameter, Object[] values) {
		delegate.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(String parameter, P[] arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(String parameter, P[] arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameterList(int parameter, Collection arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(int parameter, Collection<? extends P> arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(int parameter, Collection<? extends P> arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public Mutiny.Query<R> setParameterList(int parameter, Object[] arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(int parameter, P[] arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(int parameter, P[] arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(QueryParameter<P> parameter, P[] arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(QueryParameter<P> parameter, P[] arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P> Mutiny.Query<R> setParameterList(QueryParameter<P> parameter, P[] arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public Mutiny.Query<R> setProperties(Object bean) {
		delegate.setProperties( bean );
		return this;
	}

	@Override
	public Mutiny.Query<R> setProperties(Map bean) {
		delegate.setProperties( bean );
		return this;
	}

	@Override
	public Mutiny.Query<R> setHibernateFlushMode(FlushMode flushMode) {
		delegate.setFlushMode( FlushModeTypeHelper.getFlushModeType( flushMode ) );
		return this;
	}

	@Override
	public Integer getTimeout() {
		return delegate.getTimeout();
	}

	@Override
	public Mutiny.Query<R> setCacheable(boolean cacheable) {
		delegate.setCacheable( cacheable );
		return this;
	}

	@Override
	public Mutiny.Query<R> setCacheRegion(String cacheRegion) {
		delegate.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public Mutiny.Query<R> setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public Mutiny.Query<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		delegate.setCacheStoreMode( cacheStoreMode );
		return this;
	}

	@Override
	public Mutiny.Query<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		delegate.setCacheRetrieveMode( cacheRetrieveMode );
		return this;
	}

	@Override
	public Mutiny.Query<R> setTimeout(int timeout) {
		delegate.setTimeout( timeout );
		return this;
	}

	@Override
	public Mutiny.Query<R> setFetchSize(int fetchSize) {
		delegate.setFetchSize( fetchSize );
		return this;
	}

	@Override
	public Mutiny.Query<R> setReadOnly(boolean readOnly) {
		delegate.setReadOnly( readOnly );
		return this;
	}

	@Override
	public Mutiny.Query<R> setMaxResults(int maxResult) {
		delegate.setMaxResults( maxResult );
		return this;
	}

	@Override
	public Mutiny.Query<R> setFirstResult(int startPosition) {
		delegate.setFirstResult( startPosition );
		return this;
	}

	@Override
	public Mutiny.Query<R> setHint(String hintName, Object value) {
		delegate.setHint( hintName, value );
		return this;
	}

	@Override
	public FlushModeType getFlushMode() {
		return delegate.getFlushMode();
	}

	@Override
	public Mutiny.Query<R> setFlushMode(FlushModeType flushMode) {
		delegate.setFlushMode( flushMode );
		return this;
	}

	@Override
	public FlushMode getHibernateFlushMode() {
		return delegate.getHibernateFlushMode();
	}

	@Override
	public Mutiny.Query<R> setLockMode(LockModeType lockMode) {
		delegate.setLockMode( lockMode );
		return this;
	}
}
