/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage.impl;

import java.lang.invoke.MethodHandles;
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
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.jpa.internal.util.FlushModeTypeHelper;
import org.hibernate.metamodel.model.domain.BasicDomainType;
import org.hibernate.query.BindableType;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.ReactiveNativeQuery;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.type.BasicTypeReference;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.SingularAttribute;

/**
 * Implementation of {@link  Stage.NativeQuery}.
 */
public class StageNativeQueryImpl<R> implements Stage.NativeQuery<R> {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ReactiveNativeQuery<R> delegate;

	public StageNativeQueryImpl(ReactiveNativeQuery<R> delegate) {
		this.delegate = delegate;
	}

	@Override
	public CompletionStage<List<R>> getResultList() {
		return delegate.getReactiveResultList();
	}

	@Override
	public CompletionStage<R> getSingleResult() {
		return delegate.getReactiveSingleResult();
	}

	@Override
	public CompletionStage<R> getSingleResultOrNull() {
		return delegate.getReactiveSingleResult();
	}

	@Override
	public CompletionStage<List<R>> list() {
		return delegate.reactiveList();
	}

	@Override
	public CompletionStage<R> uniqueResult() {
		return delegate.reactiveUnique();
	}

	@Override
	public CompletionStage<Optional<R>> uniqueResultOptional() {
		return delegate.reactiveUniqueResultOptional();
	}

	@Override
	public CompletionStage<Integer> executeUpdate() {
		return delegate.executeReactiveUpdate();
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
	public Stage.NativeQuery<R> setHibernateLockMode(LockMode lockMode) {
		delegate.setHibernateLockMode( lockMode );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> setAliasSpecificLockMode(String alias, LockMode lockMode) {
		delegate.setAliasSpecificLockMode( alias, lockMode );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> setFollowOnLocking(boolean enable) {
		delegate.setFollowOnLocking( enable );
		return this;
	}

	@Override
	public String getQueryString() {
		return delegate.getQueryString();
	}

	@Override
	public  Stage.NativeQuery<R> applyGraph(RootGraph graph, GraphSemantic semantic) {
		delegate.applyGraph( graph, semantic );
		return this;
	}

	@Override
	public String getComment() {
		return delegate.getComment();
	}

	@Override
	public  Stage.NativeQuery<R> setComment(String comment) {
		delegate.setComment( comment );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> addQueryHint(String hint) {
		delegate.addQueryHint( hint );
		return this;
	}

	@Override
	public LockOptions getLockOptions() {
		return delegate.getLockOptions();
	}

	@Override
	public  Stage.NativeQuery<R> setLockOptions(LockOptions lockOptions) {
		delegate.setLockOptions( lockOptions );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setLockMode(String alias, LockMode lockMode) {
		delegate.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public <T>  Stage.NativeQuery<T> setTupleTransformer(TupleTransformer<T> transformer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public  Stage.NativeQuery<R> setResultListTransformer(ResultListTransformer<R> transformer) {
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
	public  Stage.NativeQuery<R> setParameter(String parameter, Object argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameter(String parameter, P argument, Class<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameter(String parameter, P argument, BindableType<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameter(String parameter, Instant argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameter(String parameter, Calendar argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameter(String parameter, Date argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameter(int parameter, Object argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameter(int parameter, P argument, Class<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameter(int parameter, P argument, BindableType<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameter(int parameter, Instant argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameter(int parameter, Date argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameter(int parameter, Calendar argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public <T>  Stage.NativeQuery<R> setParameter(QueryParameter<T> parameter, T argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameter(QueryParameter<P> parameter, P argument, Class<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameter(QueryParameter<P> parameter, P argument, BindableType<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <T>  Stage.NativeQuery<R> setParameter(Parameter<T> parameter, T argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameter(Parameter<Calendar> parameter, Calendar argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameter(Parameter<Date> parameter, Date argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameterList(String parameter, Collection arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(String parameter, Collection<? extends P> arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(String parameter, Collection<? extends P> arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameterList(String parameter, Object[] values) {
		delegate.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(String parameter, P[] arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(String parameter, P[] arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameterList(int parameter, Collection arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(int parameter, Collection<? extends P> arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(int parameter, Collection<? extends P> arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setParameterList(int parameter, Object[] arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(int parameter, P[] arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(int parameter, P[] arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Stage.NativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setProperties(Object bean) {
		delegate.setProperties( bean );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setProperties(Map bean) {
		delegate.setProperties( bean );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addScalar(String columnAlias) {
		delegate.addScalar( columnAlias );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addScalar(String columnAlias, BasicTypeReference type) {
		delegate.addScalar( columnAlias, type );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addScalar(String columnAlias, BasicDomainType type) {
		delegate.addScalar( columnAlias, type );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addScalar(String columnAlias, Class javaType) {
		delegate.addScalar( columnAlias, javaType );
		return this;
	}

	@Override
	public <C> Stage.NativeQuery<R> addScalar(String columnAlias, Class<C> relationalJavaType, AttributeConverter<?, C> converter) {
		delegate.addScalar( columnAlias, relationalJavaType, converter );
		return this;
	}

	@Override
	public <O, T> Stage.NativeQuery<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<T> jdbcJavaType, AttributeConverter<O, T> converter) {
		delegate.addScalar( columnAlias, domainJavaType, jdbcJavaType, converter );
		return this;
	}

	@Override
	public <C> Stage.NativeQuery<R> addScalar(String columnAlias, Class<C> relationalJavaType, Class<? extends AttributeConverter<?, C>> converter) {
		delegate.addScalar( columnAlias, relationalJavaType, converter );
		return this;
	}

	@Override
	public <O, T> Stage.NativeQuery<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<T> jdbcJavaType, Class<? extends AttributeConverter<O, T>> converter) {
		delegate.addScalar( columnAlias, domainJavaType, jdbcJavaType, converter );
		return this;
	}

	@Override
	public <J> NativeQuery.InstantiationResultNode<J> addInstantiation(Class<J> targetJavaType) {
		throw LOG.notYetImplemented();
	}

	@Override
	public Stage.NativeQuery<R> addAttributeResult(String columnAlias, Class entityJavaType, String attributePath) {
		delegate.addAttributeResult( columnAlias, entityJavaType, attributePath );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addAttributeResult(String columnAlias, String entityName, String attributePath) {
		delegate.addAttributeResult( columnAlias, entityName, attributePath );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addAttributeResult(String columnAlias, SingularAttribute attribute) {
		delegate.addAttributeResult( columnAlias, attribute );
		return this;
	}

	@Override
	public NativeQuery.RootReturn addRoot(String tableAlias, String entityName) {
		return delegate.addRoot( tableAlias, entityName );
	}

	@Override
	public NativeQuery.RootReturn addRoot(String tableAlias, Class entityType) {
		return delegate.addRoot( tableAlias, entityType );
	}

	@Override
	public Stage.NativeQuery<R> addEntity(String entityName) {
		delegate.addEntity( entityName );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addEntity(String tableAlias, String entityName) {
		delegate.addEntity( tableAlias, entityName );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addEntity(String tableAlias, String entityName, LockMode lockMode) {
		delegate.addEntity( tableAlias, entityName, lockMode );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addEntity(Class entityType) {
		delegate.addEntity( entityType );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addEntity(String tableAlias, Class entityType) {
		delegate.addEntity( tableAlias, entityType );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addEntity(String tableAlias, Class entityClass, LockMode lockMode) {
		delegate.addEntity( tableAlias, entityClass, lockMode );
		return this;
	}

	@Override
	public NativeQuery.FetchReturn addFetch(String tableAlias, String ownerTableAlias, String joinPropertyName) {
		return delegate.addFetch( tableAlias, ownerTableAlias, joinPropertyName );
	}

	@Override
	public Stage.NativeQuery<R> addJoin(String tableAlias, String path) {
		delegate.addJoin( tableAlias, path );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addJoin(String tableAlias, String ownerTableAlias, String joinPropertyName) {
		delegate.addJoin( tableAlias, ownerTableAlias, joinPropertyName );
		return this;
	}

	@Override
	public Stage.NativeQuery<R> addJoin(String tableAlias, String path, LockMode lockMode) {
		delegate.addJoin( tableAlias, path, lockMode );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setHibernateFlushMode(FlushMode flushMode) {
		delegate.setFlushMode( FlushModeTypeHelper.getFlushModeType( flushMode ) );
		return this;
	}

	@Override
	public Integer getTimeout() {
		return delegate.getTimeout();
	}

	@Override
	public  Stage.NativeQuery<R> setCacheable(boolean cacheable) {
		delegate.setCacheable( cacheable );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setCacheRegion(String cacheRegion) {
		delegate.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		delegate.setCacheStoreMode( cacheStoreMode );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		delegate.setCacheRetrieveMode( cacheRetrieveMode );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setTimeout(int timeout) {
		delegate.setTimeout( timeout );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setFetchSize(int fetchSize) {
		delegate.setFetchSize( fetchSize );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setReadOnly(boolean readOnly) {
		delegate.setReadOnly( readOnly );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setMaxResults(int maxResult) {
		delegate.setMaxResults( maxResult );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setFirstResult(int startPosition) {
		delegate.setFirstResult( startPosition );
		return this;
	}

	@Override
	public  Stage.NativeQuery<R> setHint(String hintName, Object value) {
		delegate.setHint( hintName, value );
		return this;
	}

	@Override
	public FlushModeType getFlushMode() {
		return delegate.getFlushMode();
	}

	@Override
	public  Stage.NativeQuery<R> setFlushMode(FlushModeType flushMode) {
		delegate.setFlushMode( flushMode );
		return this;
	}

	@Override
	public FlushMode getHibernateFlushMode() {
		return delegate.getHibernateFlushMode();
	}

	@Override
	public  Stage.NativeQuery<R> setLockMode(LockModeType lockMode) {
		delegate.setLockMode( lockMode );
		return this;
	}
}
