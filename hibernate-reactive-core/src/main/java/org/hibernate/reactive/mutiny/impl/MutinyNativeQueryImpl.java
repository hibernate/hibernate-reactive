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
import org.hibernate.cfg.NotYetImplementedException;
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
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.query.ReactiveNativeQuery;
import org.hibernate.type.BasicTypeReference;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.SingularAttribute;

/**
 * Implementation of {@link  Mutiny.NativeQuery}.
 */
public class MutinyNativeQueryImpl<R> implements Mutiny.NativeQuery<R> {

	private final MutinySessionFactoryImpl factory;
	private final ReactiveNativeQuery<R> delegate;

	public MutinyNativeQueryImpl(ReactiveNativeQuery<R> delegate, MutinySessionFactoryImpl factory) {
		this.delegate = delegate;
		this.factory = factory;
	}

	private <T> Uni<T> uni(Supplier<CompletionStage<T>> stageSupplier) {
		return factory.uni( stageSupplier );
	}

	@Override
	public Uni<List<R>> getResultList() {
		return uni( delegate::getReactiveResultList );
	}

	@Override
	public Uni<R> getSingleResult() {
		return uni( delegate::getReactiveSingleResult );
	}

	@Override
	public Uni<R> getSingleResultOrNull() {
		return uni( delegate::getReactiveSingleResult );
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
	public Mutiny.NativeQuery<R> setHibernateLockMode(LockMode lockMode) {
		delegate.setHibernateLockMode( lockMode );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> setAliasSpecificLockMode(String alias, LockMode lockMode) {
		delegate.setAliasSpecificLockMode( alias, lockMode );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> setFollowOnLocking(boolean enable) {
		delegate.setFollowOnLocking( enable );
		return this;
	}

	@Override
	public String getQueryString() {
		return delegate.getQueryString();
	}

	@Override
	public  Mutiny.NativeQuery<R> applyGraph(RootGraph graph, GraphSemantic semantic) {
		delegate.applyGraph( graph, semantic );
		return this;
	}

	@Override
	public String getComment() {
		return delegate.getComment();
	}

	@Override
	public  Mutiny.NativeQuery<R> setComment(String comment) {
		delegate.setComment( comment );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> addQueryHint(String hint) {
		delegate.addQueryHint( hint );
		return this;
	}

	@Override
	public LockOptions getLockOptions() {
		return delegate.getLockOptions();
	}

	@Override
	public  Mutiny.NativeQuery<R> setLockOptions(LockOptions lockOptions) {
		delegate.setLockOptions( lockOptions );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setLockMode(String alias, LockMode lockMode) {
		delegate.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public <T>  Mutiny.NativeQuery<T> setTupleTransformer(TupleTransformer<T> transformer) {
		throw new NotYetImplementedException();
	}

	@Override
	public  Mutiny.NativeQuery<R> setResultListTransformer(ResultListTransformer<R> transformer) {
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
	public  Mutiny.NativeQuery<R> setParameter(String parameter, Object argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameter(String parameter, P argument, Class<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameter(String parameter, P argument, BindableType<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameter(String parameter, Instant argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameter(String parameter, Calendar argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameter(String parameter, Date argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameter(int parameter, Object argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameter(int parameter, P argument, Class<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameter(int parameter, P argument, BindableType<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameter(int parameter, Instant argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameter(int parameter, Date argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameter(int parameter, Calendar argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public <T>  Mutiny.NativeQuery<R> setParameter(QueryParameter<T> parameter, T argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameter(QueryParameter<P> parameter, P argument, Class<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameter(QueryParameter<P> parameter, P argument, BindableType<P> type) {
		delegate.setParameter( parameter, argument, type );
		return this;
	}

	@Override
	public <T>  Mutiny.NativeQuery<R> setParameter(Parameter<T> parameter, T argument) {
		delegate.setParameter( parameter, argument );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameter(Parameter<Calendar> parameter, Calendar argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameter(Parameter<Date> parameter, Date argument, TemporalType temporalType) {
		delegate.setParameter( parameter, argument, temporalType );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameterList(String parameter, Collection arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(String parameter, Collection<? extends P> arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(String parameter, Collection<? extends P> arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameterList(String parameter, Object[] values) {
		delegate.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(String parameter, P[] arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(String parameter, P[] arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameterList(int parameter, Collection arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(int parameter, Collection<? extends P> arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(int parameter, Collection<? extends P> arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setParameterList(int parameter, Object[] arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(int parameter, P[] arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(int parameter, P[] arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] arguments) {
		delegate.setParameterList( parameter, arguments );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] arguments, Class<P> javaType) {
		delegate.setParameterList( parameter, arguments, javaType );
		return this;
	}

	@Override
	public <P>  Mutiny.NativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] arguments, BindableType<P> type) {
		delegate.setParameterList( parameter, arguments, type );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setProperties(Object bean) {
		delegate.setProperties( bean );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setProperties(Map bean) {
		delegate.setProperties( bean );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addScalar(String columnAlias) {
		delegate.addScalar( columnAlias );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addScalar(String columnAlias, BasicTypeReference type) {
		delegate.addScalar( columnAlias, type );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addScalar(String columnAlias, BasicDomainType type) {
		delegate.addScalar( columnAlias, type );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addScalar(String columnAlias, Class javaType) {
		delegate.addScalar( columnAlias, javaType );
		return this;
	}

	@Override
	public <C> Mutiny.NativeQuery<R> addScalar(String columnAlias, Class<C> relationalJavaType, AttributeConverter<?, C> converter) {
		delegate.addScalar( columnAlias, relationalJavaType, converter );
		return this;
	}

	@Override
	public <O, T> Mutiny.NativeQuery<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<T> jdbcJavaType, AttributeConverter<O, T> converter) {
		delegate.addScalar( columnAlias, domainJavaType, jdbcJavaType, converter );
		return this;
	}

	@Override
	public <C> Mutiny.NativeQuery<R> addScalar(String columnAlias, Class<C> relationalJavaType, Class<? extends AttributeConverter<?, C>> converter) {
		delegate.addScalar( columnAlias, relationalJavaType, converter );
		return this;
	}

	@Override
	public <O, T> Mutiny.NativeQuery<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<T> jdbcJavaType, Class<? extends AttributeConverter<O, T>> converter) {
		delegate.addScalar( columnAlias, domainJavaType, jdbcJavaType, converter );
		return this;
	}

	@Override
	public <J> NativeQuery.InstantiationResultNode<J> addInstantiation(Class<J> targetJavaType) {
		throw new NotYetImplementedException();
	}

	@Override
	public Mutiny.NativeQuery<R> addAttributeResult(String columnAlias, Class entityJavaType, String attributePath) {
		delegate.addAttributeResult( columnAlias, entityJavaType, attributePath );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addAttributeResult(String columnAlias, String entityName, String attributePath) {
		delegate.addAttributeResult( columnAlias, entityName, attributePath );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addAttributeResult(String columnAlias, SingularAttribute attribute) {
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
	public Mutiny.NativeQuery<R> addEntity(String entityName) {
		delegate.addEntity( entityName );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addEntity(String tableAlias, String entityName) {
		delegate.addEntity( tableAlias, entityName );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addEntity(String tableAlias, String entityName, LockMode lockMode) {
		delegate.addEntity( tableAlias, entityName, lockMode );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addEntity(Class entityType) {
		delegate.addEntity( entityType );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addEntity(String tableAlias, Class entityType) {
		delegate.addEntity( tableAlias, entityType );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addEntity(String tableAlias, Class entityClass, LockMode lockMode) {
		delegate.addEntity( tableAlias, entityClass, lockMode );
		return this;
	}

	@Override
	public NativeQuery.FetchReturn addFetch(String tableAlias, String ownerTableAlias, String joinPropertyName) {
		return delegate.addFetch( tableAlias, ownerTableAlias, joinPropertyName );
	}

	@Override
	public Mutiny.NativeQuery<R> addJoin(String tableAlias, String path) {
		delegate.addJoin( tableAlias, path );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addJoin(String tableAlias, String ownerTableAlias, String joinPropertyName) {
		delegate.addJoin( tableAlias, ownerTableAlias, joinPropertyName );
		return this;
	}

	@Override
	public Mutiny.NativeQuery<R> addJoin(String tableAlias, String path, LockMode lockMode) {
		delegate.addJoin( tableAlias, path, lockMode );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setHibernateFlushMode(FlushMode flushMode) {
		delegate.setFlushMode( FlushModeTypeHelper.getFlushModeType( flushMode ) );
		return this;
	}

	@Override
	public Integer getTimeout() {
		return delegate.getTimeout();
	}

	@Override
	public  Mutiny.NativeQuery<R> setCacheable(boolean cacheable) {
		delegate.setCacheable( cacheable );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setCacheRegion(String cacheRegion) {
		delegate.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setCacheMode(CacheMode cacheMode) {
		delegate.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		delegate.setCacheStoreMode( cacheStoreMode );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		delegate.setCacheRetrieveMode( cacheRetrieveMode );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setTimeout(int timeout) {
		delegate.setTimeout( timeout );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setFetchSize(int fetchSize) {
		delegate.setFetchSize( fetchSize );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setReadOnly(boolean readOnly) {
		delegate.setReadOnly( readOnly );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setMaxResults(int maxResult) {
		delegate.setMaxResults( maxResult );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setFirstResult(int startPosition) {
		delegate.setFirstResult( startPosition );
		return this;
	}

	@Override
	public  Mutiny.NativeQuery<R> setHint(String hintName, Object value) {
		delegate.setHint( hintName, value );
		return this;
	}

	@Override
	public FlushModeType getFlushMode() {
		return delegate.getFlushMode();
	}

	@Override
	public  Mutiny.NativeQuery<R> setFlushMode(FlushModeType flushMode) {
		delegate.setFlushMode( flushMode );
		return this;
	}

	@Override
	public FlushMode getHibernateFlushMode() {
		return delegate.getHibernateFlushMode();
	}

	@Override
	public  Mutiny.NativeQuery<R> setLockMode(LockModeType lockMode) {
		delegate.setLockMode( lockMode );
		return this;
	}
}
