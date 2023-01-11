/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.internal;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.internal.AbstractSharedSessionContract;
import org.hibernate.metamodel.model.domain.BasicDomainType;
import org.hibernate.query.BindableType;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.named.NamedResultSetMappingMemento;
import org.hibernate.query.spi.AbstractSelectionQuery;
import org.hibernate.query.spi.NonSelectQueryPlan;
import org.hibernate.query.spi.QueryInterpretationCache;
import org.hibernate.query.sql.internal.NativeQueryImpl;
import org.hibernate.query.sql.spi.NamedNativeQueryMemento;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.query.spi.ReactiveAbstractSelectionQuery;
import org.hibernate.reactive.query.sql.spi.ReactiveNativeQueryImplementor;
import org.hibernate.reactive.query.sql.spi.ReactiveNonSelectQueryPlan;
import org.hibernate.reactive.query.sqm.spi.ReactiveSelectQueryPlan;
import org.hibernate.type.BasicTypeReference;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.metamodel.SingularAttribute;

public class ReactiveNativeQueryImpl<R> extends NativeQueryImpl<R>
		implements ReactiveNativeQueryImplementor<R>  {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	private final ReactiveAbstractSelectionQuery<R> selectionQueryDelegate;

	public ReactiveNativeQueryImpl(String memento, SharedSessionContractImplementor session) {
		super( memento, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	public ReactiveNativeQueryImpl(NamedNativeQueryMemento memento, SharedSessionContractImplementor session) {
		super( memento, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	public ReactiveNativeQueryImpl(NamedNativeQueryMemento memento, Class<R> resultJavaType, SharedSessionContractImplementor session) {
		super( memento, resultJavaType, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	public ReactiveNativeQueryImpl(NamedNativeQueryMemento memento, String resultSetMappingName, SharedSessionContractImplementor session) {
		super( memento, resultSetMappingName, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	public ReactiveNativeQueryImpl(String sqlString, NamedResultSetMappingMemento resultSetMappingMemento, AbstractSharedSessionContract session) {
		super( sqlString, resultSetMappingMemento, session );
		this.selectionQueryDelegate = createSelectionQueryDelegate( session );
	}

	// Convenient for passing parameters to ReactiveAbstractSelectionQuery using method reference
	private <T> T getNull() {
		return null;
	}

	private ReactiveAbstractSelectionQuery<R> createSelectionQueryDelegate(SharedSessionContractImplementor session) {
		return new ReactiveAbstractSelectionQuery<>(
				null,
				session,
				this::doReactiveList,
				this::getNull,
				this::getNull,
				this::getNull,
				this::getNull,
				this::getQueryString,
				this::beforeQuery,
				this::afterQuery,
				AbstractSelectionQuery::uniqueElement
		);
	}
	private CompletionStage<List<R>> doReactiveList() {
		return reactiveSelectPlan().performReactiveList( this );
	}

	private ReactiveSelectQueryPlan<R> reactiveSelectPlan() {
		return (ReactiveSelectQueryPlan<R>) resolveSelectQueryPlan();
	}

	private ReactiveNonSelectQueryPlan reactiveNonSelectPlan() {
		final QueryInterpretationCache.Key cacheKey = generateNonSelectInterpretationsKey();
		if ( cacheKey != null ) {
			NonSelectQueryPlan queryPlan = getSession().getFactory().getQueryEngine()
					.getInterpretationCache().getNonSelectQueryPlan( cacheKey );
			if ( queryPlan != null ) {
				return (ReactiveNonSelectQueryPlan) queryPlan;
			}
		}

		final String sqlString = expandParameterLists();
		ReactiveNonSelectQueryPlan queryPlan = new ReactiveNativeNonSelectQueryPlan( sqlString, getQuerySpaces(), getParameterOccurrences() );
		if ( cacheKey != null ) {
			getSession().getFactory().getQueryEngine().getInterpretationCache()
					.cacheNonSelectQueryPlan( cacheKey, queryPlan );
		}

		return queryPlan;
	}

	private Set<String> getQuerySpaces() {
		// Maybe it's better to have a getter for querySpaces in the super class
		return new HashSet<>( getSynchronizedQuerySpaces() );
	}

	@Override
	public int executeUpdate() throws HibernateException {
		throw LOG.nonReactiveMethodCall( "executeReactiveUpdate" );
	}

	@Override
	public CompletionStage<Integer> executeReactiveUpdate() {
		return reactiveNonSelectPlan().executeReactiveUpdate( this );
	}

	@Override
	public R getSingleResult() {
		return selectionQueryDelegate.getSingleResult();
	}

	@Override
	public CompletionStage<R> getReactiveSingleResult() {
		return selectionQueryDelegate.getReactiveSingleResult();
	}

	@Override
	public R getSingleResultOrNull() {
		return selectionQueryDelegate.getSingleResultOrNull();
	}

	@Override
	public CompletionStage<R> getReactiveSingleResultOrNull() {
		return selectionQueryDelegate.getReactiveSingleResultOrNull();
	}

	@Override
	public CompletionStage<Optional<R>> reactiveUniqueResultOptional() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<R> reactiveUnique() {
		return selectionQueryDelegate.reactiveUnique();
	}

	@Override
	public List<R> list() {
		return selectionQueryDelegate.list();
	}

	@Override
	public CompletionStage<List<R>> reactiveList() {
		return selectionQueryDelegate.reactiveList();
	}

	@Override
	public R uniqueResult() {
		return selectionQueryDelegate.uniqueResult();
	}

	@Override
	public Optional<R> uniqueResultOptional() {
		return selectionQueryDelegate.uniqueResultOptional();
	}

	@Override
	public ReactiveNativeQueryImpl<R> applyGraph(RootGraph graph, GraphSemantic semantic) {
		super.applyGraph( graph, semantic );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> applyFetchGraph(RootGraph graph) {
		super.applyFetchGraph( graph );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addScalar(String columnAlias) {
		super.addScalar( columnAlias );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addScalar(String columnAlias, @SuppressWarnings("rawtypes") BasicDomainType type) {
		super.addScalar( columnAlias, type );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addScalar(String columnAlias, @SuppressWarnings("rawtypes") Class javaType) {
		super.addScalar( columnAlias, javaType );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addScalar(String columnAlias, BasicTypeReference type) {
		super.addScalar( columnAlias, type );
		return this;
	}

	@Override
	public <C> ReactiveNativeQueryImpl<R> addScalar(String columnAlias, Class<C> relationalJavaType, AttributeConverter<?, C> converter) {
		super.addScalar( columnAlias, relationalJavaType, converter );
		return this;
	}

	@Override
	public <O, J> ReactiveNativeQueryImpl<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<J> jdbcJavaType, AttributeConverter<O, J> converter) {
		super.addScalar( columnAlias, domainJavaType, jdbcJavaType, converter );
		return this;
	}

	@Override
	public <C> ReactiveNativeQueryImpl<R> addScalar(String columnAlias, Class<C> relationalJavaType, Class<? extends AttributeConverter<?, C>> converter) {
		super.addScalar( columnAlias, relationalJavaType, converter );
		return this;
	}

	@Override
	public <O, J> ReactiveNativeQueryImpl<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<J> jdbcJavaType, Class<? extends AttributeConverter<O, J>> converter) {
		super.addScalar( columnAlias, domainJavaType, jdbcJavaType, converter );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addAttributeResult(String columnAlias, @SuppressWarnings("rawtypes") Class entityJavaType, String attributePath) {
		super.addAttributeResult( columnAlias, entityJavaType, attributePath );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addAttributeResult(String columnAlias, String entityName, String attributePath) {
		super.addAttributeResult( columnAlias, entityName, attributePath );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addAttributeResult(String columnAlias, @SuppressWarnings("rawtypes") SingularAttribute attribute) {
		super.addAttributeResult( columnAlias, attribute );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addEntity(String entityName) {
		super.addEntity( entityName );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addEntity(String tableAlias, String entityName) {
		super.addEntity( tableAlias, entityName );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addEntity(String tableAlias, String entityName, LockMode lockMode) {
		super.addEntity( tableAlias, entityName, lockMode );
		return this;
	}
	@Override
	public ReactiveNativeQueryImpl<R> addEntity(@SuppressWarnings("rawtypes") Class entityType) {
		super.addEntity( entityType );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addEntity(String tableAlias, @SuppressWarnings("rawtypes") Class entityType) {
		super.addEntity( tableAlias, entityType );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addEntity(String tableAlias, @SuppressWarnings("rawtypes") Class entityClass, LockMode lockMode) {
		super.addEntity( tableAlias, entityClass, lockMode );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addJoin(String tableAlias, String path) {
		super.addJoin( tableAlias, path );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addJoin(String tableAlias, String ownerTableAlias, String joinPropertyName) {
		super.addJoin( tableAlias, ownerTableAlias, joinPropertyName );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addJoin(String tableAlias, String path, LockMode lockMode) {
		super.addJoin( tableAlias, path, lockMode );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addSynchronizedQuerySpace(String querySpace) {
		super.addSynchronizedQuerySpace( querySpace );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addSynchronizedEntityName(String entityName) {
		super.addSynchronizedEntityName( entityName );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addSynchronizedEntityClass(@SuppressWarnings("rawtypes") Class entityClass) {
		super.addSynchronizedEntityClass( entityClass );
		return this;
	}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// covariant overrides - Query / QueryImplementor


	@Override
	public ReactiveNativeQueryImpl<R> applyLoadGraph(RootGraph graph) {
		super.applyLoadGraph( graph );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setAliasSpecificLockMode(String alias, LockMode lockMode) {
		super.setAliasSpecificLockMode( alias, lockMode );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setHint(String hintName, Object value) {
		super.setHint( hintName, value );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setHibernateFlushMode(FlushMode flushMode) {
		super.setHibernateFlushMode( flushMode );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setFlushMode(FlushModeType flushMode) {
		super.setFlushMode( flushMode );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setFollowOnLocking(boolean enable) {
		super.setFollowOnLocking( enable );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setCacheMode(CacheMode cacheMode) {
		super.setCacheMode( cacheMode );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setCacheable(boolean cacheable) {
		super.setCacheable( cacheable );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setCacheRegion(String cacheRegion) {
		super.setCacheRegion( cacheRegion );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
		super.setCacheRetrieveMode( cacheRetrieveMode );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
		super.setCacheStoreMode( cacheStoreMode );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setTimeout(int timeout) {
		super.setTimeout( timeout );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setFetchSize(int fetchSize) {
		super.setFetchSize( fetchSize );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setReadOnly(boolean readOnly) {
		super.setReadOnly( readOnly );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setLockOptions(LockOptions lockOptions) {
		super.setLockOptions( lockOptions );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setHibernateLockMode(LockMode lockMode) {
		super.setHibernateLockMode( lockMode );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setLockMode(LockModeType lockMode) {
		super.setLockMode( lockMode );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setLockMode(String alias, LockMode lockMode) {
		super.setLockMode( alias, lockMode );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setComment(String comment) {
		super.setComment( comment );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setMaxResults(int maxResult) {
		super.setMaxResults( maxResult );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setFirstResult(int startPosition) {
		super.setFirstResult( startPosition );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> addQueryHint(String hint) {
		super.addQueryHint( hint );
		return this;
	}

	@Override
	public <T> ReactiveNativeQueryImpl<T> setTupleTransformer(TupleTransformer<T> transformer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ReactiveNativeQueryImpl<R> setResultListTransformer(ResultListTransformer<R> transformer) {
		super.setResultListTransformer( transformer );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(String name, Object val) {
		super.setParameter( name, val );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameter(String name, P val, BindableType<P> type) {
		super.setParameter( name, val, type );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameter(String name, P val, Class<P> type) {
		super.setParameter( name, val, type );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(String name, Instant value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(String name, Date value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(String name, Calendar value, TemporalType temporalType) {
		super.setParameter( name, value, temporalType );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(int position, Object val) {
		super.setParameter( position, val );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameter(int position, P val, Class<P> type) {
		super.setParameter( position, val, type );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameter(int position, P val, BindableType<P> type) {
		super.setParameter( position, val, type );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(int position, Instant value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(int position, Date value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(int position, Calendar value, TemporalType temporalType) {
		super.setParameter( position, value, temporalType );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameter(QueryParameter<P> parameter, P val) {
		super.setParameter( parameter, val );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameter(QueryParameter<P> parameter, P val, Class<P> type) {
		super.setParameter( parameter, val, type );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type) {
		super.setParameter( parameter, val, type );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameter(Parameter<P> param, P value) {
		super.setParameter( param, value );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType) {
		super.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType) {
		super.setParameter( param, value, temporalType );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameterList(String name, @SuppressWarnings("rawtypes") Collection values) {
		super.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(String name, Collection<? extends P> values, Class<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(String name, Collection<? extends P> values, BindableType<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameterList(String name, Object[] values) {
		super.setParameterList( name, values );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(String name, P[] values, Class<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(String name, P[] values, BindableType<P> type) {
		super.setParameterList( name, values, type );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values) {
		super.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(int position, Collection<? extends P> values, Class<P> type) {
		super.setParameterList( position, values, type );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(int position, Collection<? extends P> values, BindableType<P> type) {
		super.setParameterList( position, values, type );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setParameterList(int position, Object[] values) {
		super.setParameterList( position, values );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(int position, P[] values, Class<P> javaType) {
		super.setParameterList( position, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(int position, P[] values, BindableType<P> type) {
		super.setParameterList( position, values, type );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values) {
		super.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType) {
		super.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, BindableType<P> type) {
		super.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(QueryParameter<P> parameter, P[] values) {
		super.setParameterList( parameter, values );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType) {
		super.setParameterList( parameter, values, javaType );
		return this;
	}

	@Override
	public <P> ReactiveNativeQueryImpl<R> setParameterList(QueryParameter<P> parameter, P[] values, BindableType<P> type) {
		super.setParameterList( parameter, values, type );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setProperties(Object bean) {
		super.setProperties( bean );
		return this;
	}

	@Override
	public ReactiveNativeQueryImpl<R> setProperties(@SuppressWarnings("rawtypes") Map bean) {
		super.setProperties( bean );
		return this;
	}
}
