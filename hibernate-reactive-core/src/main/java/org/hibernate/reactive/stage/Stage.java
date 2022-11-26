/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.hibernate.Cache;
import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.collection.spi.AbstractPersistentCollection;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.internal.ManagedTypeHelper;
import org.hibernate.engine.spi.PersistentAttributeInterceptable;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.graph.GraphSemantic;
import org.hibernate.graph.RootGraph;
import org.hibernate.jpa.internal.util.FlushModeTypeHelper;
import org.hibernate.metamodel.model.domain.BasicDomainType;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.query.BindableType;
import org.hibernate.query.CommonQueryContract;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.QueryParameter;
import org.hibernate.query.ResultListTransformer;
import org.hibernate.query.TupleTransformer;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.common.Identifier;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.impl.ReactiveQueryExecutorLookup;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.stat.Statistics;
import org.hibernate.type.BasicTypeReference;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.TemporalType;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.metamodel.Attribute;
import jakarta.persistence.metamodel.Metamodel;
import jakarta.persistence.metamodel.SingularAttribute;

import static org.hibernate.internal.util.LockModeConverter.convertToLockMode;
import static org.hibernate.jpa.internal.util.CacheModeHelper.interpretCacheMode;
import static org.hibernate.jpa.internal.util.CacheModeHelper.interpretCacheRetrieveMode;
import static org.hibernate.jpa.internal.util.CacheModeHelper.interpretCacheStoreMode;

/**
 * An API for Hibernate Reactive where non-blocking operations are
 * represented by a Java {@link CompletionStage}.
 * <p>
 * The {@link Query}, {@link Session}, and {@link SessionFactory}
 * interfaces declared here are simply non-blocking counterparts to
 * the similarly-named interfaces in Hibernate ORM.
 */
public interface Stage {
	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	interface NativeQuery<R> extends Stage.Query<R> {

		NativeQuery<R> addScalar(String columnAlias);

		NativeQuery<R> addScalar(String columnAlias, @SuppressWarnings("rawtypes") BasicTypeReference type);

		NativeQuery<R> addScalar(String columnAlias, @SuppressWarnings("rawtypes") BasicDomainType type);

		NativeQuery<R> addScalar(String columnAlias, @SuppressWarnings("rawtypes") Class javaType);

		<C> NativeQuery<R> addScalar(String columnAlias, Class<C> relationalJavaType, AttributeConverter<?,C> converter);

		<O,T> NativeQuery<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<T> jdbcJavaType, AttributeConverter<O,T> converter);

		<C> NativeQuery<R> addScalar(String columnAlias, Class<C> relationalJavaType, Class<? extends AttributeConverter<?,C>> converter);

		<O,T> NativeQuery<R> addScalar(String columnAlias, Class<O> domainJavaType, Class<T> jdbcJavaType, Class<? extends AttributeConverter<O,T>> converter);

		<J> org.hibernate.query.NativeQuery.InstantiationResultNode<J> addInstantiation(Class<J> targetJavaType);

		NativeQuery<R> addAttributeResult(String columnAlias, @SuppressWarnings("rawtypes") Class entityJavaType, String attributePath);

		NativeQuery<R> addAttributeResult(String columnAlias, String entityName, String attributePath);

		NativeQuery<R> addAttributeResult(String columnAlias, @SuppressWarnings("rawtypes") SingularAttribute attribute);

		org.hibernate.query.NativeQuery.RootReturn addRoot(String tableAlias, String entityName);

		org.hibernate.query.NativeQuery.RootReturn addRoot(String tableAlias, @SuppressWarnings("rawtypes") Class entityType);

		NativeQuery<R> addEntity(String entityName);
		NativeQuery<R> addEntity(String tableAlias, String entityName);

		NativeQuery<R> addEntity(String tableAlias, String entityName, LockMode lockMode);
		NativeQuery<R> addEntity(@SuppressWarnings("rawtypes") Class entityType);

		NativeQuery<R> addEntity(String tableAlias, @SuppressWarnings("rawtypes") Class entityType);

		NativeQuery<R> addEntity(String tableAlias, @SuppressWarnings("rawtypes") Class entityClass, LockMode lockMode);

		org.hibernate.query.NativeQuery.FetchReturn addFetch(String tableAlias, String ownerTableAlias, String joinPropertyName);

		NativeQuery<R> addJoin(String tableAlias, String path);

		NativeQuery<R> addJoin(String tableAlias, String ownerTableAlias, String joinPropertyName);

		NativeQuery<R> addJoin(String tableAlias, String path, LockMode lockMode);

		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// covariant overrides - Query

		@Override
		NativeQuery<R> setHibernateFlushMode(FlushMode flushMode);

		@Override
		NativeQuery<R> setFlushMode(FlushModeType flushMode);

		@Override
		NativeQuery<R> setCacheMode(CacheMode cacheMode);

		@Override
		NativeQuery<R> setCacheStoreMode(CacheStoreMode cacheStoreMode);

		@Override
		NativeQuery<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode);

		@Override
		NativeQuery<R> setCacheable(boolean cacheable);

		@Override
		NativeQuery<R> setCacheRegion(String cacheRegion);

		@Override
		NativeQuery<R> setTimeout(int timeout);

		@Override
		NativeQuery<R> setFetchSize(int fetchSize);

		@Override
		NativeQuery<R> setReadOnly(boolean readOnly);

		/**
		 * @inheritDoc
		 *
		 * This operation is supported even for native queries.
		 * Note that specifying an explicit lock mode might
		 * result in changes to the native SQL query that is
		 * actually executed.
		 */
		@Override
		LockOptions getLockOptions();

		/**
		 * @inheritDoc
		 *
		 * This operation is supported even for native queries.
		 * Note that specifying an explicit lock mode might
		 * result in changes to the native SQL query that is
		 * actually executed.
		 */
		@Override
		NativeQuery<R> setLockOptions(LockOptions lockOptions);

		/**
		 * Not applicable to native SQL queries.
		 *
		 * @throws IllegalStateException for consistency with JPA
		 */
		@Override
		NativeQuery<R> setLockMode(String alias, LockMode lockMode);

		@Override
		NativeQuery<R> setComment(String comment);

		@Override
		NativeQuery<R> addQueryHint(String hint);

		@Override
		NativeQuery<R> setMaxResults(int maxResult);

		@Override
		NativeQuery<R> setFirstResult(int startPosition);

		@Override
		NativeQuery<R> setHint(String hintName, Object value);

		/**
		 * Not applicable to native SQL queries, due to an unfortunate
		 * requirement of the JPA specification.
		 * <p>
		 * Use {@link #setHibernateLockMode(LockMode)} or the hint named
		 * {@value org.hibernate.jpa.HibernateHints#HINT_NATIVE_LOCK_MODE}
		 * to set the lock mode.
		 *
		 * @throws IllegalStateException as required by JPA
		 */
		@Override
		NativeQuery<R> setLockMode(LockModeType lockMode);

		/**
		 * @inheritDoc
		 *
		 * This operation is supported even for native queries.
		 * Note that specifying an explicit lock mode might
		 * result in changes to the native SQL query that is
		 * actually executed.
		 */
		@Override
		NativeQuery<R> setHibernateLockMode(LockMode lockMode);

		@Override
		<T> NativeQuery<T> setTupleTransformer(TupleTransformer<T> transformer);

		@Override
		NativeQuery<R> setResultListTransformer(ResultListTransformer<R> transformer);

		@Override
		NativeQuery<R> setParameter(String name, Object value);

		@Override
		<P> NativeQuery<R> setParameter(String name, P val, Class<P> type);

		@Override
		<P> NativeQuery<R> setParameter(String name, P val, BindableType<P> type);

		@Override
		NativeQuery<R> setParameter(String name, Instant value, TemporalType temporalType);

		@Override
		NativeQuery<R> setParameter(String name, Calendar value, TemporalType temporalType);

		@Override
		NativeQuery<R> setParameter(String name, Date value, TemporalType temporalType);

		@Override
		NativeQuery<R> setParameter(int position, Object value);

		@Override
		<P> NativeQuery<R> setParameter(int position, P val, Class<P> type);

		@Override
		<P> NativeQuery<R> setParameter(int position, P val, BindableType<P> type);

		@Override
		NativeQuery<R> setParameter(int position, Instant value, TemporalType temporalType);

		@Override
		NativeQuery<R> setParameter(int position, Calendar value, TemporalType temporalType);

		@Override
		NativeQuery<R> setParameter(int position, Date value, TemporalType temporalType);

		@Override
		<P> NativeQuery<R> setParameter(QueryParameter<P> parameter, P val);

		@Override
		<P> NativeQuery<R> setParameter(QueryParameter<P> parameter, P val, Class<P> type);

		@Override
		<P> NativeQuery<R> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type);

		@Override
		<P> NativeQuery<R> setParameter(Parameter<P> param, P value);

		@Override
		NativeQuery<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

		@Override
		NativeQuery<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

		@Override
		NativeQuery<R> setParameterList(String name, @SuppressWarnings("rawtypes") Collection values);

		@Override
		<P> NativeQuery<R> setParameterList(String name, Collection<? extends P> values, Class<P> type);

		@Override
		<P> NativeQuery<R> setParameterList(String name, Collection<? extends P> values, BindableType<P> type);

		@Override
		NativeQuery<R> setParameterList(String name, Object[] values);

		@Override
		<P> NativeQuery<R> setParameterList(String name, P[] values, Class<P> type);

		@Override
		<P> NativeQuery<R> setParameterList(String name, P[] values, BindableType<P> type);

		@Override
		NativeQuery<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values);

		@Override
		<P> NativeQuery<R> setParameterList(int position, Collection<? extends P> values, Class<P> type);

		@Override
		<P> NativeQuery<R> setParameterList(int position, Collection<? extends P> values, BindableType<P> javaType);

		@Override
		NativeQuery<R> setParameterList(int position, Object[] values);

		@Override
		<P> NativeQuery<R> setParameterList(int position, P[] values, Class<P> javaType);

		@Override
		<P> NativeQuery<R> setParameterList(int position, P[] values, BindableType<P> javaType);

		@Override
		<P> NativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

		@Override
		<P> NativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType);

		@Override
		<P> NativeQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, BindableType<P> type);

		@Override
		<P> NativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] values);

		@Override
		<P> NativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

		@Override
		<P> NativeQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, BindableType<P> type);

		@Override
		NativeQuery<R> setProperties(Object bean);

		@Override
		NativeQuery<R> setProperties(@SuppressWarnings("rawtypes") Map bean);
	}

	/**
	 * A non-blocking counterpart to the Hibernate
	 * {@link org.hibernate.query.Query} interface, allowing reactive
	 * execution of HQL and JPQL queries.
	 * <p>
	 * The semantics of operations on this interface are identical to the
	 * semantics of the similarly-named operations of {@code Query}, except
	 * that the operations are performed asynchronously, returning a
	 * {@link CompletionStage} without blocking the calling thread.
	 * <p>
	 * Note that {@link jakarta.persistence.TemporalType} is not supported
	 * as an argument for parameter bindings, and so parameters of type
	 * {@link java.util.Date} or {@link java.util.Calendar} should not be
	 * used. Instead, datetime types from {@code java.time} should be used
	 * as parameters.
	 *
	 * @see jakarta.persistence.Query
	 */
	interface MutationQuery<R> extends CommonQueryContract {
		CompletionStage<Integer> executeUpdate();

		@Override
		Stage.MutationQuery<R> setParameter(String name, Object value);

		@Override
		<P> Stage.MutationQuery<R> setParameter(String name, P value, Class<P> type);

		@Override
		<P> Stage.MutationQuery<R> setParameter(String name, P value, BindableType<P> type);

		@Override
		Stage.MutationQuery<R> setParameter(String name, Instant value, TemporalType temporalType);

		@Override
		Stage.MutationQuery<R> setParameter(String name, Calendar value, TemporalType temporalType);

		@Override
		Stage.MutationQuery<R> setParameter(String name, Date value, TemporalType temporalType);

		@Override
		Stage.MutationQuery<R> setParameter(int position, Object value);

		@Override
		<P> Stage.MutationQuery<R> setParameter(int position, P value, Class<P> type);

		@Override
		<P> Stage.MutationQuery<R> setParameter(int position, P value, BindableType<P> type);

		@Override
		Stage.MutationQuery<R> setParameter(int position, Instant value, TemporalType temporalType);

		@Override
		Stage.MutationQuery<R> setParameter(int position, Date value, TemporalType temporalType);

		@Override
		Stage.MutationQuery<R> setParameter(int position, Calendar value, TemporalType temporalType);

		@Override
		<T> Stage.MutationQuery<R> setParameter(QueryParameter<T> parameter, T value);

		@Override
		<P> Stage.MutationQuery<R> setParameter(QueryParameter<P> parameter, P value, Class<P> type);

		@Override
		<P> Stage.MutationQuery<R> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type);

		@Override
		<T> Stage.MutationQuery<R> setParameter(Parameter<T> param, T value);

		@Override
		Stage.MutationQuery<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

		@Override
		Stage.MutationQuery<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

		@Override
		Stage.MutationQuery<R> setParameterList(String name, @SuppressWarnings("rawtypes") Collection values);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(String name, Collection<? extends P> values, BindableType<P> type);

		@Override
		Stage.MutationQuery<R> setParameterList(String name, Object[] values);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(String name, P[] values, Class<P> javaType);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(String name, P[] values, BindableType<P> type);

		@Override
		Stage.MutationQuery<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(int position, Collection<? extends P> values, BindableType<P> type);

		@Override
		Stage.MutationQuery<R> setParameterList(int position, Object[] values);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(int position, P[] values, Class<P> javaType);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(int position, P[] values, BindableType<P> type);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, BindableType<P> type);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(QueryParameter<P> parameter, P[] values);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

		@Override
		<P> Stage.MutationQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, BindableType<P> type);

		@Override
		Stage.MutationQuery<R> setProperties(Object bean);

		@Override
		Stage.MutationQuery<R> setProperties(@SuppressWarnings("rawtypes") Map bean);

		@Override
		Stage.MutationQuery<R> setHibernateFlushMode(FlushMode flushMode);
	}

	interface SelectionQuery<R> extends CommonQueryContract {

		CompletionStage<List<R>> list();

		default CompletionStage<List<R>> getResultList() {
			return list();
		}

		CompletionStage<R> getSingleResult();

		CompletionStage<R> getSingleResultOrNull();

		CompletionStage<R> uniqueResult();

		CompletionStage<Optional<R>> uniqueResultOptional();

		Stage.SelectionQuery<R> setHint(String hintName, Object value);

		@Override
		Stage.SelectionQuery<R> setFlushMode(FlushModeType flushMode);

		@Override
		Stage.SelectionQuery<R> setHibernateFlushMode(FlushMode flushMode);

		@Override
		Stage.SelectionQuery<R> setTimeout(int timeout);

		Integer getFetchSize();

		Stage.SelectionQuery<R> setFetchSize(int fetchSize);

		boolean isReadOnly();

		Stage.SelectionQuery<R> setReadOnly(boolean readOnly);

		Stage.SelectionQuery<R> setMaxResults(int maxResult);

		int getFirstResult();

		Stage.SelectionQuery<R> setFirstResult(int startPosition);

		CacheMode getCacheMode();

		CacheStoreMode getCacheStoreMode();

		CacheRetrieveMode getCacheRetrieveMode();

		Stage.SelectionQuery<R> setCacheMode(CacheMode cacheMode);

		Stage.SelectionQuery<R> setCacheStoreMode(CacheStoreMode cacheStoreMode);

		/**
		 * @see #setCacheMode(CacheMode)
		 */
		Stage.SelectionQuery<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode);

		boolean isCacheable();

		Stage.SelectionQuery<R> setCacheable(boolean cacheable);

		String getCacheRegion();

		Stage.SelectionQuery<R> setCacheRegion(String cacheRegion);

		LockOptions getLockOptions();

		LockModeType getLockMode();

		Stage.SelectionQuery<R> setLockMode(LockModeType lockMode);

		LockMode getHibernateLockMode();

		Stage.SelectionQuery<R> setHibernateLockMode(LockMode lockMode);

		Stage.SelectionQuery<R> setLockMode(String alias, LockMode lockMode);

		Stage.SelectionQuery<R> setAliasSpecificLockMode(String alias, LockMode lockMode);

		Stage.SelectionQuery<R> setFollowOnLocking(boolean enable);

		@Override
		Stage.SelectionQuery<R> setParameter(String name, Object value);

		@Override
		<P> Stage.SelectionQuery<R> setParameter(String name, P value, Class<P> type);

		@Override
		<P> Stage.SelectionQuery<R> setParameter(String name, P value, BindableType<P> type);

		@Override
		Stage.SelectionQuery<R> setParameter(String name, Instant value, TemporalType temporalType);

		@Override
		Stage.SelectionQuery<R> setParameter(String name, Calendar value, TemporalType temporalType);

		@Override
		Stage.SelectionQuery<R> setParameter(String name, Date value, TemporalType temporalType);

		@Override
		Stage.SelectionQuery<R> setParameter(int position, Object value);

		@Override
		<P> Stage.SelectionQuery<R> setParameter(int position, P value, Class<P> type);

		@Override
		<P> Stage.SelectionQuery<R> setParameter(int position, P value, BindableType<P> type);

		@Override
		Stage.SelectionQuery<R> setParameter(int position, Instant value, TemporalType temporalType);

		@Override
		Stage.SelectionQuery<R> setParameter(int position, Date value, TemporalType temporalType);

		@Override
		Stage.SelectionQuery<R> setParameter(int position, Calendar value, TemporalType temporalType);

		@Override
		<T> Stage.SelectionQuery<R> setParameter(QueryParameter<T> parameter, T value);

		@Override
		<P> Stage.SelectionQuery<R> setParameter(QueryParameter<P> parameter, P value, Class<P> type);

		@Override
		<P> Stage.SelectionQuery<R> setParameter(QueryParameter<P> parameter, P val, BindableType<P> type);

		@Override
		<T> Stage.SelectionQuery<R> setParameter(Parameter<T> param, T value);

		@Override
		Stage.SelectionQuery<R> setParameter(Parameter<Calendar> param, Calendar value, TemporalType temporalType);

		@Override
		Stage.SelectionQuery<R> setParameter(Parameter<Date> param, Date value, TemporalType temporalType);

		@Override
		Stage.SelectionQuery<R> setParameterList(String name, @SuppressWarnings("rawtypes") Collection values);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(String name, Collection<? extends P> values, Class<P> javaType);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(String name, Collection<? extends P> values, BindableType<P> type);

		@Override
		Stage.SelectionQuery<R> setParameterList(String name, Object[] values);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(String name, P[] values, Class<P> javaType);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(String name, P[] values, BindableType<P> type);

		@Override
		Stage.SelectionQuery<R> setParameterList(int position, @SuppressWarnings("rawtypes") Collection values);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(int position, Collection<? extends P> values, Class<P> javaType);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(int position, Collection<? extends P> values, BindableType<P> type);

		@Override
		Stage.SelectionQuery<R> setParameterList(int position, Object[] values);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(int position, P[] values, Class<P> javaType);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(int position, P[] values, BindableType<P> type);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, Class<P> javaType);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> values, BindableType<P> type);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(QueryParameter<P> parameter, P[] values);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, Class<P> javaType);

		@Override
		<P> Stage.SelectionQuery<R> setParameterList(QueryParameter<P> parameter, P[] values, BindableType<P> type);

		@Override
		Stage.SelectionQuery<R> setProperties(Object bean);

		@Override
		Stage.SelectionQuery<R> setProperties(@SuppressWarnings("rawtypes") Map bean);
	}

	interface Query<R> extends Stage.SelectionQuery<R>, Stage.MutationQuery<R> {
		String getQueryString();

		Stage.Query<R> applyGraph(@SuppressWarnings("rawtypes") RootGraph graph, GraphSemantic semantic);

		default Stage.Query<R> applyFetchGraph(@SuppressWarnings("rawtypes") RootGraph graph) {
			return applyGraph( graph, GraphSemantic.FETCH );
		}

		@SuppressWarnings("UnusedDeclaration")
		default Stage.Query<R> applyLoadGraph(@SuppressWarnings("rawtypes") RootGraph graph) {
			return applyGraph( graph, GraphSemantic.LOAD );
		}

		String getComment();

		Stage.Query<R> setComment(String comment);

		Stage.Query<R> addQueryHint(String hint);

		@Override
		LockOptions getLockOptions();

		Stage.Query<R> setLockOptions(LockOptions lockOptions);

		@Override
		Stage.Query<R> setLockMode(String alias, LockMode lockMode);

		<T> Stage.Query<T> setTupleTransformer(TupleTransformer<T> transformer);

		Stage.Query<R> setResultListTransformer(ResultListTransformer<R> transformer);

		QueryOptions getQueryOptions();

		ParameterMetadata getParameterMetadata();

		@Override
		Stage.Query<R> setParameter(String parameter, Object argument);

		@Override
		<P> Stage.Query<R> setParameter(String parameter, P argument, Class<P> type);

		@Override
		<P> Stage.Query<R> setParameter(String parameter, P argument, BindableType<P> type);

		/**
		 * Bind an {@link Instant} value to the named query parameter using
		 * just the portion indicated by the given {@link TemporalType}.
		 */
		Stage.Query<R> setParameter(String parameter, Instant argument, TemporalType temporalType);

		@Override
		Stage.Query<R> setParameter(String parameter, Calendar argument, TemporalType temporalType);

		@Override
		Stage.Query<R> setParameter(String parameter, Date argument, TemporalType temporalType);

		/**
		 * Bind the given argument to an ordinal query parameter.
		 * <p>
		 * If the type of the parameter cannot be inferred from the context in
		 * which it occurs, use one of the forms which accepts a "type".
		 *
		 * @see #setParameter(int, Object, Class)
		 * @see #setParameter(int, Object, BindableType)
		 */
		@Override
		Stage.Query<R> setParameter(int parameter, Object argument);

		/**
		 * Bind the given argument to an ordinal query parameter using the given
		 * Class reference to attempt to determine the {@link BindableType}
		 * to use.  If unable to determine an appropriate {@link BindableType},
		 * {@link #setParameter(int, Object)} is used.
		 *
		 * @see BindableType#parameterType(Class)
		 * @see #setParameter(int, Object, BindableType)
		 */
		<P> Stage.Query<R> setParameter(int parameter, P argument, Class<P> type);

		/**
		 * Bind the given argument to an ordinal query parameter using the given
		 * {@link BindableType}.
		 *
		 * @see BindableType#parameterType
		 */
		<P> Stage.Query<R> setParameter(int parameter, P argument, BindableType<P> type);

		/**
		 * Bind an {@link Instant} value to the ordinal query parameter using
		 * just the portion indicated by the given {@link TemporalType}.
		 */
		Stage.Query<R> setParameter(int parameter, Instant argument, TemporalType temporalType);

		/**
		 * {@link jakarta.persistence.Query} override
		 */
		@Override
		Stage.Query<R> setParameter(int parameter, Date argument, TemporalType temporalType);

		/**
		 * {@link jakarta.persistence.Query} override
		 */
		@Override
		Stage.Query<R> setParameter(int parameter, Calendar argument, TemporalType temporalType);

		<T> Stage.Query<R> setParameter(QueryParameter<T> parameter, T argument);

		<P> Stage.Query<R> setParameter(QueryParameter<P> parameter, P argument, Class<P> type);

		<P> Stage.Query<R> setParameter(QueryParameter<P> parameter, P argument, BindableType<P> type);

		@Override
		<T> Stage.Query<R> setParameter(Parameter<T> parameter, T argument);

		@Override
		Stage.Query<R> setParameter(Parameter<Calendar> parameter, Calendar argument, TemporalType temporalType);

		@Override
		Stage.Query<R> setParameter(Parameter<Date> parameter, Date argument, TemporalType temporalType);

		Stage.Query<R> setParameterList(String parameter, @SuppressWarnings("rawtypes") Collection arguments);

		<P> Stage.Query<R> setParameterList(String parameter, Collection<? extends P> arguments, Class<P> javaType);

		/**
		 * Bind multiple arguments to a named query parameter using the given
		 * {@link BindableType}.
		 *
		 * @apiNote This is used for binding a list of values to an expression
		 * such as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(String parameter, Collection<? extends P> arguments, BindableType<P> type);


		/**
		 * Bind multiple arguments to a named query parameter.
		 * <p/>
		 * The "type mapping" for the binding is inferred from the type of
		 * the first collection element.
		 *
		 * @apiNote This is used for binding a list of values to an expression s
		 * uch as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		Stage.Query<R> setParameterList(String parameter, Object[] values);

		/**
		 * Bind multiple arguments to a named query parameter using the given
		 * Class reference to attempt to determine the {@link BindableType}
		 * to use.  If unable to determine an appropriate {@link BindableType},
		 * {@link #setParameterList(String, Collection)} is used.
		 *
		 * @see BindableType#parameterType(Class)
		 * @see #setParameterList(java.lang.String, Object[], BindableType)
		 *
		 * @apiNote This is used for binding a list of values to an expression
		 * such as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(String parameter, P[] arguments, Class<P> javaType);


		/**
		 * Bind multiple arguments to a named query parameter using the given
		 * {@link BindableType}.
		 *
		 * @apiNote This is used for binding a list of values to an expression
		 * such as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(String parameter, P[] arguments, BindableType<P> type);

		/**
		 * Bind multiple arguments to an ordinal query parameter.
		 * <p/>
		 * The "type mapping" for the binding is inferred from the type of
		 * the first collection element.
		 *
		 * @apiNote This is used for binding a list of values to an expression
		 * such as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		Stage.Query<R> setParameterList(int parameter, @SuppressWarnings("rawtypes") Collection arguments);

		/**
		 * Bind multiple arguments to an ordinal query parameter using the given
		 * Class reference to attempt to determine the {@link BindableType}
		 * to use.  If unable to determine an appropriate {@link BindableType},
		 * {@link #setParameterList(String, Collection)} is used.
		 *
		 * @see BindableType#parameterType(Class)
		 * @see #setParameterList(int, Collection, BindableType)
		 *
		 * @apiNote This is used for binding a list of values to an expression
		 * such as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(int parameter, Collection<? extends P> arguments, Class<P> javaType);

		/**
		 * Bind multiple arguments to an ordinal query parameter using the given
		 * {@link BindableType}.
		 *
		 * @apiNote This is used for binding a list of values to an expression
		 * such as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(int parameter, Collection<? extends P> arguments, BindableType<P> type);

		/**
		 * Bind multiple arguments to an ordinal query parameter.
		 * <p>
		 * The "type mapping" for the binding is inferred from the type of the
		 * first collection element.
		 *
		 * @apiNote This is used for binding a list of values to an expression
		 * such as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		Stage.Query<R> setParameterList(int parameter, Object[] arguments);

		/**
		 * Bind multiple arguments to an ordinal query parameter using the given
		 * {@link Class} reference to attempt to determine the {@link BindableType}
		 * to use. If unable to determine an appropriate {@link BindableType},
		 * {@link #setParameterList(String, Collection)} is used.
		 *
		 * @see BindableType#parameterType(Class)
		 * @see #setParameterList(int, Object[], BindableType)
		 *
		 * @apiNote This is used for binding a list of values to an expression
		 * such as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(int parameter, P[] arguments, Class<P> javaType);

		/**
		 * Bind multiple arguments to an ordinal query parameter using the given
		 * {@link BindableType}.
		 *
		 * @apiNote This is used for binding a list of values to an expression
		 * such as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(int parameter, P[] arguments, BindableType<P> type);

		/**
		 * Bind multiple arguments to the query parameter represented by the given
		 * {@link QueryParameter}.
		 * <p>
		 * The type of the parameter is inferred from the context in which it occurs,
		 * and from the type of the first given argument.
		 *
		 * @param parameter the parameter memento
		 * @param arguments a collection of arguments
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments);

		/**
		 * Bind multiple arguments to the query parameter represented by the given
		 * {@link QueryParameter} using the given Class reference to attempt to
		 * determine the {@link BindableType} to use. If unable to determine an
		 * appropriate {@link BindableType}, {@link #setParameterList(String, Collection)}
		 * is used.
		 *
		 * @see BindableType#parameterType(Class)
		 * @see #setParameterList(QueryParameter, java.util.Collection, BindableType)
		 *
		 * @apiNote This is used for binding a list of values to an expression such
		 * as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments, Class<P> javaType);

		/**
		 * Bind multiple arguments to the query parameter represented by the given
		 * {@link QueryParameter}, inferring the {@link BindableType}.
		 * <p>
		 * The "type mapping" for the binding is inferred from the type of the first
		 * collection element.
		 *
		 * @apiNote This is used for binding a list of values to an expression such
		 * as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(QueryParameter<P> parameter, Collection<? extends P> arguments, BindableType<P> type);

		/**
		 * Bind multiple arguments to the query parameter represented by the
		 * given {@link QueryParameter}
		 * <p>
		 * The type of the parameter is inferred between the context in which it
		 * occurs, the type associated with the QueryParameter and the type of
		 * the first given argument.
		 *
		 * @param parameter the parameter memento
		 * @param arguments a collection of arguments
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(QueryParameter<P> parameter, P[] arguments);

		/**
		 * Bind multiple arguments to the query parameter represented by the
		 * given {@link QueryParameter} using the given Class reference to attempt
		 * to determine the {@link BindableType} to use.  If unable to
		 * determine an appropriate {@link BindableType},
		 * {@link #setParameterList(String, Collection)} is used
		 *
		 * @see BindableType#parameterType(Class)
		 * @see #setParameterList(QueryParameter, Object[], BindableType)
		 *
		 * @apiNote This is used for binding a list of values to an expression such
		 * as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(QueryParameter<P> parameter, P[] arguments, Class<P> javaType);

		/**
		 * Bind multiple arguments to the query parameter represented by the
		 * given {@link QueryParameter}, inferring the {@link BindableType}.
		 * <p>
		 * The "type mapping" for the binding is inferred from the type of
		 * the first collection element
		 *
		 * @apiNote This is used for binding a list of values to an expression such
		 * as {@code entity.field in (:values)}.
		 *
		 * @return {@code this}, for method chaining
		 */
		<P> Stage.Query<R> setParameterList(QueryParameter<P> parameter, P[] arguments, BindableType<P> type);

		/**
		 * Bind the property values of the given bean to named parameters of the query,
		 * matching property names with parameter names and mapping property types to
		 * Hibernate types using heuristics.
		 *
		 * @param bean any JavaBean or POJO
		 *
		 * @return {@code this}, for method chaining
		 */
		Stage.Query<R> setProperties(Object bean);

		/**
		 * Bind the values of the given Map for each named parameters of the query,
		 * matching key names with parameter names and mapping value types to
		 * Hibernate types using heuristics.
		 *
		 * @param bean a {@link Map} of names to arguments
		 *
		 * @return {@code this}, for method chaining
		 */
		Stage.Query<R> setProperties(@SuppressWarnings("rawtypes") Map bean);


		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// covariant overrides - CommonQueryContract

		@Override
		Stage.Query<R> setHibernateFlushMode(FlushMode flushMode);

		@Override
		Stage.Query<R> setCacheable(boolean cacheable);

		@Override
		Stage.Query<R> setCacheRegion(String cacheRegion);

		@Override
		Stage.Query<R> setCacheMode(CacheMode cacheMode);

		@Override
		Stage.Query<R> setCacheStoreMode(CacheStoreMode cacheStoreMode);

		@Override
		Stage.Query<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode);

		@Override
		Stage.Query<R> setTimeout(int timeout);

		@Override
		Stage.Query<R> setFetchSize(int fetchSize);

		@Override
		Stage.Query<R> setReadOnly(boolean readOnly);


		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		// covariant overrides - jakarta.persistence.Query/TypedQuery

		@Override
		Stage.Query<R> setMaxResults(int maxResult);

		@Override
		Stage.Query<R> setFirstResult(int startPosition);

		@Override
		Stage.Query<R> setHint(String hintName, Object value);

		@Override
		Stage.Query<R> setFlushMode(FlushModeType flushMode);

		@Override
		Stage.Query<R> setLockMode(LockModeType lockMode);
	}

	/**
	 * A non-blocking counterpart to the Hibernate {@link org.hibernate.Session}
	 * interface, allowing a reactive style of interaction with the database.
	 * <p>
	 * The semantics of operations on this interface are identical to the
	 * semantics of the similarly-named operations of {@code Session}, except
	 * that the operations are performed asynchronously, returning a
	 * {@link CompletionStage} without blocking the calling thread.
	 * <p>
	 * Entities associated with an {@code Session} do not support transparent
	 * lazy association fetching. Instead, {@link #fetch(Object)} should be used
	 * to explicitly request asynchronous fetching of an association, or the
	 * association should be fetched eagerly when the entity is first retrieved,
	 * for example, by:
	 *
	 * <ul>
	 * <li>{@link #enableFetchProfile(String) enabling a fetch profile},
	 * <li>using an {@link EntityGraph}, or
	 * <li>writing a {@code join fetch} clause in a HQL query.
	 * </ul>
	 *
	 * @see org.hibernate.Session
	 */
	interface Session extends Closeable {

		/**
		 * Asynchronously return the persistent instance of the given entity
		 * class with the given identifier, or {@code null} if there is no such
		 * persistent instance. If the instance is already associated with
		 * the session, return the associated instance. This method never
		 * returns an uninitialized instance.
		 *
		 * <pre>
		 * {@code session.find(Book.class, id).thenAccept(book -> print(book.getTitle()));}
		 * </pre>
		 *
		 * @param entityClass The entity type
		 * @param id an identifier
		 *
		 * @return a persistent instance or null via a {@code CompletionStage}
		 *
		 * @see jakarta.persistence.EntityManager#find(Class, Object)
		 */
		<T> CompletionStage<T> find(Class<T> entityClass, Object id);

		/**
		 * Asynchronously return the persistent instance of the given entity
		 * class with the given identifier, requesting the given {@link LockMode}.
		 *
		 * @param entityClass The entity type
		 * @param id an identifier
		 * @param lockMode the requested {@link LockMode}
		 *
		 * @return a persistent instance or null via a {@code CompletionStage}
		 *
		 * @see #find(Class,Object)
		 * @see #lock(Object, LockMode) this discussion of lock modes
		 */
		<T> CompletionStage<T> find(Class<T> entityClass, Object id, LockMode lockMode);

		/**
		 * Asynchronously return the persistent instance of the given entity
		 * class with the given identifier, requesting the given {@link LockModeType}.
		 *
		 * @param entityClass The entity type
		 * @param id an identifier
		 * @param lockModeType the requested {@link LockModeType}
		 *
		 * @return a persistent instance or null via a {@code CompletionStage}
		 *
		 * @see #find(Class,Object)
		 * @see #lock(Object, LockMode) this discussion of lock modes
		 */
		default <T> CompletionStage<T> find(Class<T> entityClass, Object id, LockModeType lockModeType) {
			return find( entityClass, id, convertToLockMode(lockModeType) );
		}

//		/**
//		 * Asynchronously return the persistent instance of the given entity
//		 * class with the given identifier, requesting the given {@link LockOptions}.
//		 *
//		 * @param entityClass The entity type
//		 * @param id an identifier
//		 * @param lockOptions the requested {@link LockOptions}
//		 *
//		 * @return a persistent instance or null via a {@code CompletionStage}
//		 *
//		 * @see #find(Class,Object)
//		 * @see #lock(Object, LockMode) this discussion of lock modes
//		 */
//		<T> CompletionStage<T> find(Class<T> entityClass, Object id, LockOptions lockOptions);

		 /**
		 * Asynchronously return the persistent instance with the given
		 * identifier of an entity class, using the given {@link EntityGraph}
		 * as a fetch plan.
		 *
		 * @param entityGraph an {@link EntityGraph} specifying the entity
		 *                    and associations to be fetched
		 * @param id an identifier
		 *
		 * @see #find(Class,Object)
		 */
		<T> CompletionStage<T> find(EntityGraph<T> entityGraph, Object id);

		/**
		 * Asynchronously return the persistent instances of the given entity
		 * class with the given identifiers, or null if there is no such
		 * persistent instance.
		 *
		 * @param entityClass The entity type
		 * @param ids the identifiers
		 *
		 * @return a list of persistent instances and nulls via a {@code CompletionStage}
		 */
		<T> CompletionStage<List<T>> find(Class<T> entityClass, Object... ids);

		/**
		 * Asynchronously return the persistent instance of the given entity
		 * class with the given natural identifiers, or null if there is no
		 * such persistent instance.
		 *
		 * @param entityClass The entity type
		 * @param naturalId the natural identifier
		 *
		 * @return a persistent instance or null via a {@code CompletionStage}
		 */
		@Incubating
		<T> CompletionStage<T> find(Class<T> entityClass, Identifier<T> naturalId);

		/**
		 * Return the persistent instance of the given entity class with the
		 * given identifier, assuming that the instance exists. This method
		 * never results in access to the underlying data store, and thus
		 * might return a proxy that must be initialized explicitly using
		 * {@link #fetch(Object)}.
		 * <p>
		 * You should not use this method to determine if an instance exists
		 * (use {@link #find} instead). Use this only to retrieve an instance
		 * which you safely assume exists, where non-existence would be an
		 * actual error.
		 *
		 * @param entityClass a persistent class
		 * @param id a valid identifier of an existing persistent instance of the class
		 *
		 * @return the persistent instance or proxy
		 *
		 * @see jakarta.persistence.EntityManager#getReference(Class, Object)
		 */
		<T> T getReference(Class<T> entityClass, Object id);

		/**
		 * Return the persistent instance with the same identity as the given
		 * instance, which might be detached, assuming that the instance is
		 * still persistent in the database. This method never results in
		 * access to the underlying data store, and thus might return a proxy
		 * that must be initialized explicitly using {@link #fetch(Object)}.
		 *
		 * @param entity a detached persistent instance
		 *
		 * @return the persistent instance or proxy
		 */
		<T> T getReference(T entity);

		/**
		 * Asynchronously persist the given transient instance, first assigning
		 * a generated identifier. (Or using the current value of the identifier
		 * property if the entity has assigned identifiers.)
		 * <p>
		 * This operation cascades to associated instances if the association is
		 * mapped with {@link jakarta.persistence.CascadeType#PERSIST}.
		 *
		 * <pre>
		 * {@code session.persist(newBook).thenAccept(v -> session.flush());}
		 * </pre>
		 *
		 * @param entity a transient instance of a persistent class
		 *
		 * @see jakarta.persistence.EntityManager#persist(Object)
		 */
		CompletionStage<Void> persist(Object entity);

		/**
		 * Persist multiple transient entity instances at once.
		 *
		 * @see #persist(Object)
		 */
		CompletionStage<Void> persist(Object... entities);

		/**
		 * Asynchronously remove a persistent instance from the datastore. The
		 * argument may be an instance associated with the receiving session or
		 * a transient instance with an identifier associated with existing
		 * persistent state.
		 * <p>
		 * This operation cascades to associated instances if the association is
		 * mapped with {@link jakarta.persistence.CascadeType#REMOVE}.
		 *
		 * <pre>
		 * {@code session.delete(book).thenAccept(v -> session.flush());}
		 * </pre>
		 *
		 * @param entity the managed persistent instance to be removed
		 *
		 * @throws IllegalArgumentException if the given instance is not managed
		 *
		 * @see jakarta.persistence.EntityManager#remove(Object)
		 */
		CompletionStage<Void> remove(Object entity);

		/**
		 * Remove multiple entity instances at once.
		 *
		 * @see #remove(Object)
		 */
		CompletionStage<Void> remove(Object... entities);

		/**
		 * Copy the state of the given object onto the persistent instance with
		 * the same identifier. If there is no such persistent instance currently
		 * associated with the session, it will be loaded. Return the persistent
		 * instance. Or, if the given instance is transient, save a copy of it
		 * and return the copy as a newly persistent instance. The given instance
		 * does not become associated with the session.
		 * <p>
		 * This operation cascades to associated instances if the association is
		 * mapped with {@link jakarta.persistence.CascadeType#MERGE}.
		 *
		 * @param entity a detached instance with state to be copied
		 *
		 * @return an updated persistent instance
		 *
		 * @see jakarta.persistence.EntityManager#merge(Object)
		 */
		<T> CompletionStage<T> merge(T entity);

		/**
		 * Merge multiple entity instances at once.
		 *
		 * @see #merge(Object)
		 */
		<T> CompletionStage<Void> merge(T... entities);

		/**
		 * Re-read the state of the given instance from the underlying database.
		 * It is inadvisable to use this to implement long-running sessions that
		 * span many business tasks. This method is, however, useful in certain
		 * special circumstances, for example:
		 *
		 * <ul>
		 * <li>where a database trigger alters the object state after insert or
		 * update, or
		 * <li>after executing direct native SQL in the same session.
		 * </ul>
		 *
		 * @param entity a managed persistent instance
		 *
		 * @throws IllegalArgumentException if the given instance is not managed
		 *
		 * @see jakarta.persistence.EntityManager#refresh(Object)
		 */
		CompletionStage<Void> refresh(Object entity);

		/**
		 * Re-read the state of the given instance from the underlying database,
		 * requesting the given {@link LockMode}.
		 *
		 * @param entity a managed persistent entity instance
		 * @param lockMode the requested lock mode
		 *
		 * @see #refresh(Object)
		 */
		CompletionStage<Void> refresh(Object entity, LockMode lockMode);

		/**
		 * Re-read the state of the given instance from the underlying database,
		 * requesting the given {@link LockModeType}.
		 *
		 * @param entity a managed persistent entity instance
		 * @param lockModeType the requested lock mode
		 *
		 * @see #refresh(Object)
		 */
		default CompletionStage<Void> refresh(Object entity, LockModeType lockModeType) {
			return refresh( entity, convertToLockMode(lockModeType) );
		}

//		/**
//		 * Re-read the state of the given instance from the underlying database,
//		 * requesting the given {@link LockOptions}.
//		 *
//		 * @param entity a managed persistent entity instance
//		 * @param lockOptions the requested {@link LockOptions}
//		 *
//		 * @see #refresh(Object)
//		 */
//		CompletionStage<Void> refresh(Object entity, LockOptions lockOptions);

		/**
		 * Refresh multiple entity instances at once.
		 *
		 * @see #refresh(Object)
		 */
		CompletionStage<Void> refresh(Object... entities);

		/**
		 * Obtain the specified lock level upon the given object. For example,
		 * this operation may be used to:
		 *
		 * <ul>
		 * <li>perform a version check with {@link LockMode#PESSIMISTIC_READ},
		 * <li>upgrade to a pessimistic lock with {@link LockMode#PESSIMISTIC_WRITE},
		 * <li>force a version increment with {@link LockMode#PESSIMISTIC_FORCE_INCREMENT},
		 * <li>schedule a version check just before the end of the transaction with
		 * {@link LockMode#OPTIMISTIC}, or
		 * <li>schedule a version increment just before the end of the transaction
		 * with {@link LockMode#OPTIMISTIC_FORCE_INCREMENT}.
		 * </ul>
		 *
		 * This operation cascades to associated instances if the association is
		 * mapped with {@link org.hibernate.annotations.CascadeType#LOCK}.
		 *
		 * @param entity a managed persistent instance
		 * @param lockMode the lock level
		 *
		 * @throws IllegalArgumentException if the given instance is not managed
		 */
		CompletionStage<Void> lock(Object entity, LockMode lockMode);

		/**
		 * Obtain the specified lock level upon the given object. For example,
		 * this operation may be used to:
		 *
		 * <ul>
		 * <li>perform a version check with {@link LockModeType#PESSIMISTIC_READ},
		 * <li>upgrade to a pessimistic lock with {@link LockModeType#PESSIMISTIC_WRITE},
		 * <li>force a version increment with {@link LockModeType#PESSIMISTIC_FORCE_INCREMENT},
		 * <li>schedule a version check just before the end of the transaction with
		 * {@link LockModeType#OPTIMISTIC}, or
		 * <li>schedule a version increment just before the end of the transaction
		 * with {@link LockModeType#OPTIMISTIC_FORCE_INCREMENT}.
		 * </ul>
		 *
		 * This operation cascades to associated instances if the association is
		 * mapped with {@link org.hibernate.annotations.CascadeType#LOCK}.
		 *
		 * @param entity a managed persistent instance
		 * @param lockModeType the lock level
		 *
		 * @throws IllegalArgumentException if the given instance is not managed
		 */
		default CompletionStage<Void> lock(Object entity, LockModeType lockModeType) {
			return lock( entity, convertToLockMode(lockModeType) );
		}

//		/**
//		 * Obtain the specified lock level upon the given object, with the given
//		 * {@link LockOptions}.
//		 *
//		 * @param entity a managed persistent instance
//		 * @param lockOptions the requested {@link LockOptions}
//		 *
//		 * @throws IllegalArgumentException if the given instance is not managed
//		 */
//		CompletionStage<Void> lock(Object entity, LockOptions lockOptions);

		/**
		 * Force this session to flush asynchronously. Must be called at the
		 * end of a unit of work, before committing the transaction and closing
		 * the session. <i>Flushing</i> is the process of synchronizing the
		 * underlying persistent store with state held in memory.
		 *
		 * <pre>
		 * {@code session.flush().thenAccept(v -> print("done saving changes"));}
		 * </pre>
		 *
		 * @see jakarta.persistence.EntityManager#flush()
		 */
		CompletionStage<Void> flush();

		/**
		 * Asynchronously fetch an association that's configured for lazy loading.
		 *
		 * <pre>
		 * {@code session.fetch(author.getBook()).thenAccept(book -> print(book.getTitle()))}
		 * </pre>
		 *
		 * @param association a lazy-loaded association
		 *
		 * @return the fetched association, via a {@code CompletionStage}
		 *
		 * @see Stage#fetch(Object)
		 * @see org.hibernate.Hibernate#initialize(Object)
		 */
		<T> CompletionStage<T> fetch(T association);

		/**
		 * Fetch a lazy property of the given entity, identified by a JPA
		 * {@link Attribute attribute metamodel}. Note that this feature is
		 * only supported in conjunction with the Hibernate bytecode enhancer.
		 *
		 * <pre>
		 * {@code session.fetch(book, Book_.isbn).thenAccept(isbn -> print(isbn))}
		 * </pre>
		 */
		<E,T> CompletionStage<T> fetch(E entity, Attribute<E,T> field);

		/**
		 * Asynchronously fetch an association that's configured for lazy loading,
		 * and unwrap the underlying entity implementation from any proxy.
		 *
		 * <pre>
		 * {@code session.unproxy(author.getBook()).thenAccept(book -> print(book.getTitle()));}
		 * </pre>
		 *
		 * @param association a lazy-loaded association
		 *
		 * @return the fetched association, via a {@code CompletionStage}
		 *
		 * @see org.hibernate.Hibernate#unproxy(Object)
		 */
		<T> CompletionStage<T> unproxy(T association);

		/**
		 * Determine the current lock mode of the given entity.
		 */
		LockMode getLockMode(Object entity);

		/**
		 * Determine if the given instance belongs to this persistence context.
		 */
		boolean contains(Object entity);

		<R> Query<R> createSelectQuery(String queryString);

		/**
		 * Create an instance of {@link Query} for the given HQL/JPQL query
		 * string or HQL/JPQL update or delete statement. In the case of an
		 * update or delete, the returned {@link Query} must be executed using
		 * {@link Query#executeUpdate()} which returns an affected row count.
		 *
		 * @param queryString The HQL/JPQL query, update or delete statement
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String)
		 */
		<R> Query<R> createQuery(String queryString);

		/**
		 * Create an instance of {@link Query} for the given HQL/JPQL query
		 * string.
		 *
		 * @param queryString The HQL/JPQL query
		 * @param resultType the Java type returned in each row of query results
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String, Class)
		 */
		<R> Query<R> createQuery(String queryString, Class<R> resultType);

		/**
		 * Create an instance of {@link Query} for the given criteria update.
		 *
		 * @param criteriaUpdate The {@link CriteriaUpdate}
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 */
		<R> MutationQuery<R> createQuery(CriteriaUpdate<R> criteriaUpdate);

		/**
		 * Create an instance of {@link Stage.Query} for the given criteria delete.
		 *
		 * @param criteriaDelete The {@link CriteriaDelete}
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 */
		<R> MutationQuery<R> createQuery(CriteriaDelete<R> criteriaDelete);

		/**
		 * Create an instance of {@link Query} for the named query.
		 *
		 * @param queryName The name of the query
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String)
		 */
		<R> Query<R> createNamedQuery(String queryName);

		/**
		 * Create an instance of {@link Query} for the named query.
		 *
		 * @param queryName The name of the query
		 * @param resultType the Java type returned in each row of query results
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String, Class)
		 */
		<R> Query<R> createNamedQuery(String queryName, Class<R> resultType);

		/**
		 * Create an instance of {@link Query} for the given SQL query string,
		 * using the given {@code resultType} to interpret the results.
		 *
		 * <ul>
		 * <li>If the given result type is {@link Object}, or a built-in type
		 * such as {@link String} or {@link Integer}, the result set must
		 * have a single column, which will be returned as a scalar.</li>
		 * <li>If the given result type is {@code Object[]}, then the result set
		 * must have multiple columns, which will be returned as elements of
		 * arrays of type {@code Object[]}.</li>
		 * <li>Otherwise, the given result type must be an entity class, in which
		 * case the result set column aliases must map to the fields of the
		 * entity, and the query will return instances of the entity.</li>
		 * </ul>
		 *
		 * @param queryString The SQL query
		 * @param resultType the Java type returned in each row of query results
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createNativeQuery(String, Class)
		 */
		<R> Query<R> createNativeQuery(String queryString, Class<R> resultType);


		/**
		 * Create an instance of {@link Query} for the given SQL query string,
		 * using the given {@code resultType} to interpret the results.
		 *
		 * <ul>
		 * <li>If the given result type is {@link Object}, or a built-in type
		 * such as {@link String} or {@link Integer}, the result set must
		 * have a single column, which will be returned as a scalar.</li>
		 * <li>If the given result type is {@code Object[]}, then the result set
		 * must have multiple columns, which will be returned as elements of
		 * arrays of type {@code Object[]}.</li>
		 * <li>Otherwise, the given result type must be an entity class, in which
		 * case the result set column aliases must map to the fields of the
		 * entity, and the query will return instances of the entity.</li>
		 * </ul>
		 *
		 * Any {@link AffectedEntities affected entities} are synchronized with
		 * the database before execution of the query.
		 *
		 * @param queryString The SQL query
		 * @param resultType the Java type returned in each row of query results
		 * @param affectedEntities The entities which are affected by the query
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createNativeQuery(String, Class)
		 */
		<R> Query<R> createNativeQuery(String queryString, Class<R> resultType, AffectedEntities affectedEntities);

		/**
		 * Create an instance of {@link Query} for the given SQL query string,
		 * using the given {@link ResultSetMapping} to interpret the result set.
		 *
		 * @param queryString The SQL query
		 * @param resultSetMapping the result set mapping
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see #getResultSetMapping(Class, String)
		 * @see jakarta.persistence.EntityManager#createNativeQuery(String, String)
		 */
		<R> Query<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping);

		/**
		 * Create an instance of {@link Query} for the given SQL query string,
		 * using the given {@link ResultSetMapping} to interpret the result set.
		 * <p>
		 * Any {@link AffectedEntities affected entities} are synchronized with
		 * the database before execution of the query.
		 *
		 * @param queryString The SQL query
		 * @param resultSetMapping the result set mapping
		 * @param affectedEntities The entities which are affected by the query
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see #getResultSetMapping(Class, String)
		 * @see jakarta.persistence.EntityManager#createNativeQuery(String, String)
		 */
		<R> Query<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping, AffectedEntities affectedEntities);

		/**
		 * Create an instance of {@link Query} for the given SQL query string,
		 * or SQL update, insert, or delete statement. In the case of an update,
		 * insert, or delete, the returned {@link Query} must be executed using
		 * {@link Query#executeUpdate()} which returns an affected row count.
		 * In the case of a query:
		 *
		 * <ul>
		 * <li>If the result set has a single column, the results will be returned
		 * as scalars.</li>
		 * <li>Otherwise, if the result set has multiple columns, the results will
		 * be returned as elements of arrays of type {@code Object[]}.</li>
		 * </ul>
		 *
		 * @param queryString The SQL select, update, insert, or delete statement
		 */
		<R> Query<R> createNativeQuery(String queryString);

		/**
		 * Create an instance of {@link Query} for the given SQL query string,
		 * or SQL update, insert, or delete statement. In the case of an update,
		 * insert, or delete, the returned {@link Query} must be executed using
		 * {@link Query#executeUpdate()} which returns an affected row count.
		 * In the case of a query:
		 *
		 * <ul>
		 * <li>If the result set has a single column, the results will be returned
		 * as scalars.</li>
		 * <li>Otherwise, if the result set has multiple columns, the results will
		 * be returned as elements of arrays of type {@code Object[]}.</li>
		 * </ul>
		 *
		 * Any {@link AffectedEntities affected entities} are synchronized with
		 * the database before execution of the statement.
		 *
		 * @param queryString The SQL select, update, insert, or delete statement
		 * @param affectedEntities The entities which are affected by the statement
		 */
		<R> Query<R> createNativeQuery(String queryString, AffectedEntities affectedEntities);

		/**
		 * Create an instance of {@link Query} for the given criteria query.
		 *
		 * @param criteriaQuery The {@link CriteriaQuery}
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String)
		 */
		<R> Query<R> createQuery(CriteriaQuery<R> criteriaQuery);

		/**
		 * Set the {@link FlushMode flush mode} for this session.
		 * <p>
		 * The flush mode determines the points at which the session is flushed.
		 * <i>Flushing</i> is the process of synchronizing the underlying persistent
		 * store with persistable state held in memory.
		 * <p>
		 * For a logically "read only" session, it is reasonable to set the session's
		 * flush mode to {@link FlushMode#MANUAL} at the start of the session (in
		 * order to achieve some extra performance).
		 *
		 * @param flushMode the new flush mode
		 */
		Session setFlushMode(FlushMode flushMode);

		/**
		 * Set the {@link FlushModeType flush mode} for this session.
		 * <p>
		 * The flush mode determines the points at which the session is flushed.
		 * <i>Flushing</i> is the process of synchronizing the underlying persistent
		 * store with persistable state held in memory.
		 *
		 * @param flushModeType the new flush mode
		 */
		default Session setFlushMode(FlushModeType flushModeType) {
			return setFlushMode( FlushModeTypeHelper.getFlushMode(flushModeType) );
		}

		/**
		 * Get the current flush mode for this session.
		 *
		 * @return the flush mode
		 */
		FlushMode getFlushMode();
		/**
		 * Remove this instance from the session cache. Changes to the instance
		 * will not be synchronized with the database.
		 * <p>
		 * This operation cascades to associated instances if the association is
		 * mapped with {@link jakarta.persistence.CascadeType#DETACH}.
		 *
		 * @param entity The entity to evict
		 *
		 * @throws NullPointerException if the passed object is {@code null}
		 * @throws IllegalArgumentException if the passed object is not defined as an entity
		 *
		 * @see jakarta.persistence.EntityManager#detach(Object)
		 */
		Session detach(Object entity);

		/**
		 * Completely clear the session. Detach all persistent instances and cancel
		 * all pending insertions, updates and deletions.
		 *
		 * @see jakarta.persistence.EntityManager#clear()
		 */
		Session clear();

		/**
		 * Enable a particular fetch profile on this session, or do nothing if
		 * requested fetch profile is already enabled.
		 *
		 * @param name The name of the fetch profile to be enabled.
		 * @throws org.hibernate.UnknownProfileException Indicates that the given name does not
		 * match any known profile names
		 *
		 * @see org.hibernate.engine.profile.FetchProfile for discussion of this feature
		 */
		Session enableFetchProfile(String name);

		/**
		 * Obtain a native SQL result set mapping defined via the annotation
		 * {@link jakarta.persistence.SqlResultSetMapping}.
		 */
		<T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName);

		/**
		 * Obtain a named {@link EntityGraph}
		 */
		<T> EntityGraph<T> getEntityGraph(Class<T> rootType, String graphName);

		/**
		 * Create a new mutable {@link EntityGraph}
		 */
		<T> EntityGraph<T> createEntityGraph(Class<T> rootType);

		/**
		 * Create a new mutable copy of a named {@link EntityGraph}
		 */
		<T> EntityGraph<T> createEntityGraph(Class<T> rootType, String graphName);

		/**
		 * Disable a particular fetch profile on this session, or do nothing if
		 * the requested fetch profile is not enabled.
		 *
		 * @param name The name of the fetch profile to be disabled.
		 * @throws org.hibernate.UnknownProfileException Indicates that the given name does not
		 * match any known profile names
		 *
		 * @see org.hibernate.engine.profile.FetchProfile for discussion of this feature
		 */
		Session disableFetchProfile(String name);

		/**
		 * Determine if the fetch profile with the given name is enabled for this
		 * session.
		 *
		 * @param name The name of the profile to be checked.
		 * @return True if fetch profile is enabled; false if not.
		 * @throws org.hibernate.UnknownProfileException Indicates that the given name does not
		 * match any known profile names
		 *
		 * @see org.hibernate.engine.profile.FetchProfile for discussion of this feature
		 */
		boolean isFetchProfileEnabled(String name);

		/**
		 * Change the default for entities and proxies loaded into this session
		 * from modifiable to read-only mode, or from modifiable to read-only mode.
		 * <p>
		 * Read-only entities are not dirty-checked and snapshots of persistent
		 * state are not maintained. Read-only entities can be modified, but
		 * changes are not persisted.
		 *
		 * @see org.hibernate.Session#setDefaultReadOnly(boolean)
		 */
		 Session setDefaultReadOnly(boolean readOnly);

		/**
		 * @return the default read-only mode for entities and proxies loaded in
		 *         this session
		 */
		boolean isDefaultReadOnly();

		/**
		 * Set an unmodified persistent object to read-only mode, or a read-only
		 * object to modifiable mode. In read-only mode, no snapshot is maintained,
		 * the instance is never dirty checked, and changes are not persisted.
		 *
		 * @see org.hibernate.Session#setReadOnly(Object, boolean)
		 */
		 Session setReadOnly(Object entityOrProxy, boolean readOnly);

		/**
		 * Is the specified entity or proxy read-only?
		 *
		 * @see org.hibernate.Session#isReadOnly(Object)
		 */
		boolean isReadOnly(Object entityOrProxy);

		/**
		 * Set the {@link CacheMode cache mode} for this session.
		 * <p>
		 * The cache mode determines the manner in which this session interacts
		 * with the second level cache.
		 *
		 * @param cacheMode The new cache mode.
		 */
		Session setCacheMode(CacheMode cacheMode);

		/**
		 * Set the {@link CacheStoreMode} for this session.
		 *
		 * @param cacheStoreMode The new cache store mode.
		 */
		default Session setCacheStoreMode(CacheStoreMode cacheStoreMode) {
			return setCacheMode( interpretCacheMode( cacheStoreMode, interpretCacheRetrieveMode(getCacheMode()) ) );
		}

		/**
		 * Set the {@link CacheRetrieveMode} for this session.
		 *
		 * @param cacheRetrieveMode The new cache retrieve mode.
		 */
		default Session setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
			return setCacheMode( interpretCacheMode( interpretCacheStoreMode(getCacheMode()), cacheRetrieveMode ) );
		}

		/**
		 * Get the current cache mode.
		 *
		 * @return The current cache mode.
		 */
		CacheMode getCacheMode();

		/**
		 * Set the session-level batch size, overriding the batch size set
		 * by the configuration property {@code hibernate.jdbc.batch_size}.
		 */
		Session setBatchSize(Integer batchSize);

		/**
		 * The session-level batch size, or {@code null} if it has not been
		 * overridden.
		 */
		Integer getBatchSize();

		/**
		 * Enable the named filter for this session.
		 *
		 * @param filterName The name of the filter to be enabled.
		 *
		 * @return The Filter instance representing the enabled filter.
		 */
		Filter enableFilter(String filterName);

		/**
		 * Disable the named filter for this session.
		 *
		 * @param filterName The name of the filter to be disabled.
		 */
		void disableFilter(String filterName);

		/**
		 * Retrieve a currently enabled filter by name.
		 *
		 * @param filterName The name of the filter to be retrieved.
		 *
		 * @return The Filter instance representing the enabled filter.
		 */
		Filter getEnabledFilter(String filterName);

		/**
		 * Performs the given work within the scope of a database transaction,
		 * automatically flushing the session. The transaction will be rolled
		 * back if the work completes with an uncaught exception, or if
		 * {@link Transaction#markForRollback()} is called.
		 * <p>
		 * The resulting {@link Transaction} object may also be obtained via
		 * {@link #currentTransaction()}.
		 * <p>
		 * <il>
		 * <li> If there is already a transaction associated with this session,
		 * the work is executed in the context of the existing transaction, and
		 * no new transaction is initiated.
		 * <li> If there is no transaction associated with this session, a new
		 * transaction is started, and the work is executed in the context of
		 * the new transaction.
		 * </il>
		 *
		 * @param work a function which accepts {@link Transaction} and returns
		 *             the result of the work as a {@link CompletionStage}.
		 *
		 * @see SessionFactory#withTransaction(BiFunction)
		 */
		<T> CompletionStage<T> withTransaction(Function<Transaction, CompletionStage<T>> work);

		/**
		 * Obtain the transaction currently associated with this session,
		 * if any.
		 *
		 * @return the {@link Transaction}, or null if no transaction
		 * was started using {@link #withTransaction(Function)}.
		 *
		 * @see #withTransaction(Function)
		 * @see SessionFactory#withTransaction(BiFunction)
		 */
		Transaction currentTransaction();

		/**
		 * @return false if {@link #close()} has been called
		 */
		boolean isOpen();
	}

	/**
	 * A non-blocking counterpart to the Hibernate
	 * {@link org.hibernate.StatelessSession} interface, which provides a
	 * command-oriented API for performing bulk operations against a database.
	 * <p>
	 * A stateless session does not implement a first-level cache nor interact
	 * with any second-level cache, nor does it implement transactional
	 * write-behind or automatic dirty checking, nor do operations cascade to
	 * associated instances. Changes to many to many associations and element
	 * collections may not be made persistent in a stateless session.
	 * Operations performed via a stateless session bypass Hibernate's event
	 * model and interceptors.
	 * <p>
	 * For certain kinds of work, a stateless session may perform slightly
	 * better than a stateful session.
	 * <p>
	 * In particular, for a session which loads many entities, use of a
	 * {@code StatelessSession} alleviates the need to call:
	 * <ul>
	 * <li>{@link Session#clear()} or {@link Session#detach(Object)} to perform
	 * first-level cache management, and
	 * <li>{@link Session#setCacheMode(CacheMode)} to bypass interaction with
	 * the second-level cache.
	 * </ul>
	 * <p>
	 * Stateless sessions are vulnerable to data aliasing effects, due to the
	 * lack of a first-level cache.
	 *
	 * @see org.hibernate.StatelessSession
	 */
	interface StatelessSession extends Closeable {

		/**
		 * Retrieve a row.
		 *
		 * @param entityClass The class of the entity to retrieve
		 * @param id The id of the entity to retrieve
		 *
		 * @return a detached entity instance, via a {@code CompletionStage}
		 *
		 * @see org.hibernate.StatelessSession#get(Class, Object)
		 */
		<T> CompletionStage<T> get(Class<T> entityClass, Object id);

		/**
		 * Retrieve a row, obtaining the specified lock mode.
		 *
		 * @param entityClass The class of the entity to retrieve
		 * @param id The id of the entity to retrieve
		 * @param lockMode The lock mode to apply to the entity
		 *
		 * @return a detached entity instance, via a {@code CompletionStage}
		 *
		 * @see org.hibernate.StatelessSession#get(Class, Object, LockMode)
		 */
		<T> CompletionStage<T> get(Class<T> entityClass, Object id, LockMode lockMode);

		/**
		 * Retrieve a row, obtaining the specified lock mode.
		 *
		 * @param entityClass The class of the entity to retrieve
		 * @param id The id of the entity to retrieve
		 * @param lockModeType The lock mode to apply to the entity
		 *
		 * @return a detached entity instance, via a {@code CompletionStage}
		 *
		 * @see org.hibernate.StatelessSession#get(Class, Object, LockMode)
		 */
		default <T> CompletionStage<T> get(Class<T> entityClass, Object id, LockModeType lockModeType) {
			return get( entityClass, id, convertToLockMode(lockModeType) );
		}

		/**
		 * Retrieve a row, using the given {@link EntityGraph} as a fetch plan.
		 *
		 * @param entityGraph an {@link EntityGraph} specifying the entity
		 *                    and associations to be fetched
		 * @param id The id of the entity to retrieve
		 *
		 * @return a detached entity instance, via a {@code CompletionStage}
		 */
		<T> CompletionStage<T> get(EntityGraph<T> entityGraph, Object id);

		/**
		 * Create an instance of {@link Query} for the given HQL/JPQL query
		 * string or HQL/JPQL update or delete statement. In the case of an
		 * update or delete, the returned {@link Query} must be executed using
		 * {@link Query#executeUpdate()} which returns an affected row count.
		 *
		 * @param queryString The HQL/JPQL query, update or delete statement
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see org.hibernate.Session#createQuery(String)
		 */
		<R> Query<R> createQuery(String queryString);

		/**
		 * Create an instance of {@link Query} for the given HQL/JPQL query
		 * string.
		 *
		 * @param queryString The HQL/JPQL query
		 * @param resultType the Java type returned in each row of query results
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see org.hibernate.Session#createQuery(String, Class)
		 */
		<R> Query<R> createQuery(String queryString, Class<R> resultType);

		/**
		 * Create an instance of {@link Query} for the given SQL query string,
		 * or SQL update, insert, or delete statement. In the case of an update,
		 * insert, or delete, the returned {@link Query} must be executed using
		 * {@link Query#executeUpdate()} which returns an affected row count.
		 *
		 * @param queryString The SQL select, update, insert, or delete statement
		 *
		 * @see org.hibernate.Session#createNativeQuery(String)
		 */
		<R> Query<R> createNativeQuery(String queryString);

		/**
		 * Create an instance of {@link Query} for the named query.
		 *
		 * @param queryName The name of the query
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String)
		 */
		<R> Query<R> createNamedQuery(String queryName);

		/**
		 * Create an instance of {@link Query} for the named query.
		 *
		 * @param queryName The name of the query
		 * @param resultType the Java type returned in each row of query results
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String, Class)
		 */
		<R> Query<R> createNamedQuery(String queryName, Class<R> resultType);

		/**
		 * Create an instance of {@link Query} for the given SQL query string,
		 * using the given {@code resultType} to interpret the results.
		 *
		 * @param queryString The SQL query
		 * @param resultType the Java type returned in each row of query results
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see org.hibernate.Session#createNativeQuery(String, Class)
		 */
		<R> Query<R> createNativeQuery(String queryString, Class<R> resultType);

		/**
		 * Create an instance of {@link Query} for the given criteria query.
		 *
		 * @param criteriaQuery The {@link CriteriaQuery}
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String)
		 */
		<R> Query<R> createQuery(CriteriaQuery<R> criteriaQuery);

		/**
		 * Create an instance of {@link Query} for the given criteria update.
		 *
		 * @param criteriaUpdate The {@link CriteriaUpdate}
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String)
		 */
		<R> Stage.MutationQuery<R> createQuery(CriteriaUpdate<R> criteriaUpdate);

		/**
		 * Create an instance of {@link Query} for the given criteria delete.
		 *
		 * @param criteriaDelete The {@link CriteriaDelete}
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String)
		 */
		<R> Stage.MutationQuery<R> createQuery(CriteriaDelete<R> criteriaDelete);

		/**
		 * Insert a row.
		 *
		 * @param entity a new transient instance
		 *
		 * @see org.hibernate.StatelessSession#insert(Object)
		 */
		CompletionStage<Void> insert(Object entity);

		/**
		 * Insert multiple rows.
		 *
		 * @param entities new transient instances
		 *
		 * @see org.hibernate.StatelessSession#insert(Object)
		 */
		CompletionStage<Void> insert(Object... entities);

		/**
		 * Insert multiple rows.
		 *
		 * @param batchSize the batch size
		 * @param entities new transient instances
		 *
		 * @see org.hibernate.StatelessSession#insert(Object)
		 */
		CompletionStage<Void> insert(int batchSize, Object... entities);

		/**
		 * Delete a row.
		 *
		 * @param entity a detached entity instance
		 *
		 * @see org.hibernate.StatelessSession#delete(Object)
		 */
		CompletionStage<Void> delete(Object entity);

		/**
		 * Delete multiple rows.
		 *
		 * @param entities detached entity instances
		 *
		 * @see org.hibernate.StatelessSession#delete(Object)
		 */
		CompletionStage<Void> delete(Object... entities);

		/**
		 * Delete multiple rows.
		 *
		 * @param batchSize the batch size
		 * @param entities detached entity instances
		 *
		 * @see org.hibernate.StatelessSession#delete(Object)
		 */
		CompletionStage<Void> delete(int batchSize, Object... entities);

		/**
		 * Update a row.
		 *
		 * @param entity a detached entity instance
		 *
		 * @see org.hibernate.StatelessSession#update(Object)
		 */
		CompletionStage<Void> update(Object entity);

		/**
		 * Update multiple rows.
		 *
		 * @param entities a detached entity instance
		 *
		 * @see org.hibernate.StatelessSession#update(Object)
		 */
		CompletionStage<Void> update(Object... entities);

		/**
		 * Update multiple rows.
		 *
		 * @param batchSize the batch size
		 * @param entities a detached entity instance
		 *
		 * @see org.hibernate.StatelessSession#update(Object)
		 */
		CompletionStage<Void> update(int batchSize, Object... entities);

		/**
		 * Refresh the entity instance state from the database.
		 *
		 * @param entity The entity to be refreshed.
		 *
		 * @see org.hibernate.StatelessSession#refresh(Object)
		 */
		CompletionStage<Void> refresh(Object entity);

		/**
		 * Refresh the entity instance state from the database.
		 *
		 * @param entities The entities to be refreshed.
		 *
		 * @see org.hibernate.StatelessSession#refresh(Object)
		 */
		CompletionStage<Void> refresh(Object... entities);

		/**
		 * Refresh the entity instance state from the database.
		 *
		 * @param batchSize the batch size
		 * @param entities The entities to be refreshed.
		 *
		 * @see org.hibernate.StatelessSession#refresh(Object)
		 */
		CompletionStage<Void> refresh(int batchSize, Object... entities);

		/**
		 * Refresh the entity instance state from the database.
		 *
		 * @param entity The entity to be refreshed.
		 * @param lockMode The LockMode to be applied.
		 *
		 * @see org.hibernate.StatelessSession#refresh(Object, LockMode)
		 */
		CompletionStage<Void> refresh(Object entity, LockMode lockMode);

		/**
		 * Refresh the entity instance state from the database.
		 *
		 * @param entity The entity to be refreshed.
		 * @param lockModeType The LockMode to be applied.
		 *
		 * @see org.hibernate.StatelessSession#refresh(Object, LockMode)
		 */
		default CompletionStage<Void> refresh(Object entity, LockModeType lockModeType) {
			return refresh( entity, convertToLockMode(lockModeType) );
		}

		/**
		 * Asynchronously fetch an association that's configured for lazy loading.
		 *
		 * <pre>
		 * {@code session.fetch(author.getBook()).thenAccept(book -> print(book.getTitle()))}
		 * </pre>
		 *
		 * Warning: this operation in a stateless session is quite sensitive to
		 * data aliasing effects and should be used with great care.
		 *
		 * @param association a lazy-loaded association
		 *
		 * @return the fetched association, via a {@code CompletionStage}
		 *
		 * @see org.hibernate.Hibernate#initialize(Object)
		 */
		<T> CompletionStage<T> fetch(T association);

		/**
		 * Obtain a native SQL result set mapping defined via the annotation
		 * {@link jakarta.persistence.SqlResultSetMapping}.
		 */
		<T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName);

		/**
		 * Obtain a named {@link EntityGraph}
		 */
		<T> EntityGraph<T> getEntityGraph(Class<T> rootType, String graphName);

		/**
		 * Create a new mutable {@link EntityGraph}
		 */
		<T> EntityGraph<T> createEntityGraph(Class<T> rootType);

		/**
		 * Create a new mutable copy of a named {@link EntityGraph}
		 */
		<T> EntityGraph<T> createEntityGraph(Class<T> rootType, String graphName);

		/**
		 * Performs the given work within the scope of a database transaction,
		 * automatically flushing the session. The transaction will be rolled
		 * back if the work completes with an uncaught exception, or if
		 * {@link Transaction#markForRollback()} is called.
		 * <il>
		 * <li> If there is already a transaction associated with this session,
		 * the work is executed in the context of the existing transaction, and
		 * no new transaction is initiated.
		 * <li> If there is no transaction associated with this session, a new
		 * transaction is started, and the work is executed in the context of
		 * the new transaction.
		 * </il>
		 *
		 * @param work a function which accepts {@link Transaction} and returns
		 *             the result of the work as a {@link CompletionStage}.
		 *
		 * @see SessionFactory#withTransaction(BiFunction)
		 */
		<T> CompletionStage<T> withTransaction(Function<Transaction, CompletionStage<T>> work);

		/**
		 * Obtain the transaction currently associated with this session,
		 * if any.
		 *
		 * @return the {@link Transaction}, or null if no transaction
		 * was started using {@link #withTransaction(Function)}.
		 *
		 * @see #withTransaction(Function)
		 * @see SessionFactory#withTransaction(BiFunction)
		 */
		Transaction currentTransaction();

		/**
		 * @return false if {@link #close()} has been called
		 */
		boolean isOpen();

		/**
		 * Close the reactive session and release the underlying database
		 * connection.
		 */
		CompletionStage<Void> close();
	}

	/**
	 * Allows code within {@link Session#withTransaction(Function)} to mark a
	 * transaction for rollback. A transaction marked for rollback will
	 * never be committed.
	 */
	interface Transaction {
		/**
		 * Mark the current transaction for rollback.
		 */
		void markForRollback();
		/**
		 * Is the current transaction marked for rollback.
		 */
		boolean isMarkedForRollback();
	}

	/**
	 * Factory for {@link Session reactive sessions}.
	 * <p>
	 * A {@code Stage.SessionFactory} may be obtained from an instance of
	 * {@link jakarta.persistence.EntityManagerFactory} as follows:
	 *
	 * <pre>
	 * Stage.SessionFactory sessionFactory =
	 * 			createEntityManagerFactory("example")
	 * 				.unwrap(Stage.SessionFactory.class);
	 * </pre>
	 *
	 * Here, configuration properties must be specified in
	 * {@code persistence.xml}.
	 * <p>
	 * Alternatively, a {@code Stage.SessionFactory} may be obtained via
	 * programmatic configuration of Hibernate using:
	 *
	 * <pre>
	 * Configuration configuration = new Configuration();
	 * ...
	 * Stage.SessionFactory sessionFactory =
	 * 		configuration.buildSessionFactory(
	 * 			new ReactiveServiceRegistryBuilder()
	 * 				.applySettings( configuration.getProperties() )
	 * 				.build()
	 * 		)
	 * 		.unwrap(Stage.SessionFactory.class);
	 * </pre>
	 *
	 */
	interface SessionFactory extends AutoCloseable {

		/**
		 * Obtain a new {@link Session reactive session} {@link CompletionStage}, the main
		 * interaction point between the user's program and Hibernate
		 * Reactive.
		 * <p>
		 * When the {@link CompletionStage} completes successfully it returns a newly created session.
		 * <p>
		 * The client must explicitly close the session by calling
		 * {@link Session#close()}.
		 *
		 * @see #withSession(Function)
		 */
		CompletionStage<Session> openSession();

		/**
		 * Obtain a new {@link Session reactive session} {@link CompletionStage} for a
		 * specified tenant.
		 * <p>
		 * When the {@link CompletionStage} completes successfully it returns a newly created session.
		 * <p>
		 * The client must explicitly close the session by calling
		 * {@link Session#close()}.
		 *
		 * @param tenantId the id of the tenant
		 *
		 * @see #withSession(Function)
		 */
		CompletionStage<Session> openSession(String tenantId);

		/**
		 * Obtain a {@link StatelessSession reactive stateless session}
		 *{@link CompletionStage}.
		 * <p>
		 * When the {@link CompletionStage} completes successfully it returns a newly created session.
		 * <p>
		 * The client must explicitly close the session by calling
		 * {@link StatelessSession#close()}.
		 */
		CompletionStage<StatelessSession> openStatelessSession();

		/**
		 * Obtain a {@link StatelessSession reactive stateless session}
		 * {@link CompletionStage}.
		 * <p>
		 * When the {@link CompletionStage} completes successfully it returns a newly created session.
		 * <p>
		 * The client must explicitly close the session by calling
		 * {@link StatelessSession#close()}.
		 *
		 * @param tenantId the id of the tenant
		 */
		CompletionStage<StatelessSession> openStatelessSession(String tenantId);

		/**
		 * Perform work using a {@link Session reactive session}.
		 * <p>
		 * <il>
		 * <li>If there is already a session associated with the current
		 * reactive stream, then the work will be executed using that
		 * session.
		 * <li>Otherwise, if there is no session associated with the
		 * current stream, a new session will be created.
		 * </il>
		 * <p>
		 * The session will be closed automatically, but must be flushed
		 * explicitly if necessary.
		 *
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link CompletionStage}.
		 */
		<T> CompletionStage<T> withSession(Function<Session, CompletionStage<T>> work);

		/**
		 * Perform work using a {@link Session reactive session} for a
		 * specified tenant.
		 * <p>
		 * <il>
		 * <li>If there is already a session associated with the current
		 * reactive stream, and the given tenant, then the work will be
		 * executed using that session.
		 * <li>Otherwise, a new session will be created.
		 * </il>
		 * <p>
		 * The session will be closed automatically, but must be flushed
		 * explicitly if necessary.
		 *
		 * @param tenantId the id of the tenant
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link CompletionStage}.
		 */
		<T> CompletionStage<T> withSession(String tenantId, Function<Session, CompletionStage<T>> work);

		/**
		 * Perform work using a {@link Session reactive session} within an
		 * associated {@link Transaction transaction}.
		 * <p>
		 * <il>
		 * <li>If there is already a stateless session associated with the
		 * current reactive stream, then the work will be executed using that
		 * session.
		 * <li>Otherwise, if there is no stateless session associated with the
		 * current stream, a new stateless session will be created.
		 * </il>
		 * <p>
		 * The session will be {@link Session#flush() flushed} and closed
		 * automatically, and the transaction committed automatically.
		 *
		 * @param work a function which accepts the session and transaction
		 *             and returns the result of the work as a
		 *             {@link CompletionStage}.
		 *
		 * @see #withSession(Function)
		 * @see Session#withTransaction(Function)
		 */
		<T> CompletionStage<T> withTransaction(BiFunction<Session, Transaction, CompletionStage<T>> work);

		/**
		 * Perform work using a {@link Session reactive session} within an
		 * associated transaction.
		 * <p>
		 * <il>
		 * <li>If there is already a session associated with the
		 * current reactive stream, then the work will be executed using that
		 * session.
		 * <li>Otherwise, if there is no stateless session associated with the
		 * current stream, a new stateless session will be created.
		 * </il>
		 * <p>
		 * The session will be {@link Session#flush() flushed} and closed
		 * automatically, and the transaction committed automatically.
		 *
		 * @param work a function which accepts the session and returns the
		 *             result of the work as a {@link CompletionStage}.
		 *
		 * @see #withTransaction(BiFunction)
		 * @see Session#withTransaction(Function)
		 */
		default <T> CompletionStage<T> withTransaction(Function<Session, CompletionStage<T>> work) {
			return withTransaction( (session, transaction) -> work.apply( session ) );
		}

		/**
		 * Perform work using a {@link Session reactive session} for a
		 * specified tenant within an associated {@link Transaction transaction}.
		 * <p>
		 * <il>
		 * <li>If there is already a session associated with the
		 * current reactive stream and the given tenant, then the work will be executed using that
		 * session.
		 * <li>Otherwise, if there is no stateless session associated with the
		 * current stream and the given tenant, a new stateless session will be created.
		 * </il>
		 * <p>
		 * The session will be {@link Session#flush() flushed} and closed
		 * automatically, and the transaction committed automatically.
		 *
		 * @param tenantId the id of the tenant
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link CompletionStage}.
		 *
		 * @see #withSession(String, Function)
		 * @see Session#withTransaction(Function)
		 */
		<T> CompletionStage<T> withTransaction(String tenantId, BiFunction<Session, Transaction, CompletionStage<T>> work);

		/**
		 * Perform work using a {@link StatelessSession reactive session} within an
		 * associated {@link Transaction transaction}.
		 * <p>
		 * <il>
		 * <li>If there is already a stateless session associated with the
		 * current reactive stream, then the work will be executed using that
		 * session.
		 * <li>Otherwise, if there is no stateless session associated with the
		 * current stream, a new stateless session will be created.
		 * </il>
		 * <p>
		 * The session will be closed automatically, and the transaction committed automatically.
		 *
		 * @param work a function which accepts the stateless session and returns
		 *             the result of the work as a {@link CompletionStage}.
		 *
		 * @see #withStatelessSession(Function)
		 * @see StatelessSession#withTransaction(Function)
		 */
		default <T> CompletionStage<T> withStatelessTransaction(Function<StatelessSession, CompletionStage<T>> work) {
			return withStatelessTransaction( ( (statelessSession, transaction) -> work.apply( statelessSession ) ) );
		}

		/**
		 * Perform work using a {@link StatelessSession reactive session} within an
		 * associated {@link Transaction transaction}.
		 * <p>
		 * <il>
		 * <li>If there is already a stateless session associated with the
		 * current reactive stream, then the work will be executed using that
		 * session.
		 * <li>Otherwise, if there is no stateless session associated with the
		 * current stream, a new stateless session will be created.
		 * </il>
		 * <p>
		 * The session will be closed automatically, and the transaction committed automatically.
		 *
		 * @param work a function which accepts the stateless session and returns
		 *             the result of the work as a {@link CompletionStage}.
		 *
		 * @see #withStatelessSession(Function)
		 * @see StatelessSession#withTransaction(Function)
		 */
		<T> CompletionStage<T> withStatelessTransaction(BiFunction<StatelessSession, Transaction, CompletionStage<T>> work);

		/**
		 * Perform work using a {@link StatelessSession reactive session} within an
		 * associated {@link Transaction transaction}.
		 * <p>
		 * <il>
		 * <li>If there is already a stateless session associated with the
		 * current reactive stream and the given tenant, then the work will be executed using that
		 * session.
		 * <li>Otherwise, if there is no stateless session associated with the
		 * current stream, a new stateless session will be created.
		 * </il>
		 * <p>
		 * The session will be closed automatically, and the transaction committed automatically.
		 *
		 * @param tenantId the id of the tenant
		 * @param work a function which accepts the stateless session and returns
		 *             the result of the work as a {@link CompletionStage}.
		 *
		 * @see #withStatelessSession(String, Function)
		 * @see StatelessSession#withTransaction(Function)
		 */
		<T> CompletionStage<T> withStatelessTransaction(String tenantId, BiFunction<StatelessSession, Transaction, CompletionStage<T>> work);

		/**
		 * Perform work using a {@link StatelessSession stateless session}.
		 * <p>
		 * <il>
		 * <li>If there is already a stateless session associated with the
		 * current reactive stream, then the work will be executed using that
		 * session.
		 * <li>Otherwise, if there is no stateless session associated with the
		 * current stream, a new stateless session will be created.
		 * </il>
		 * <p>
		 * The session will be closed automatically.
		 *
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link CompletionStage}.
		 */
		<T> CompletionStage<T> withStatelessSession(Function<StatelessSession, CompletionStage<T>> work);

		/**
		 * Perform work using a {@link StatelessSession stateless session}.
		 * <p>
		 * <il>
		 * <li>If there is already a stateless session associated with the
		 * current reactive stream, then the work will be executed using that
		 * session.
		 * <li>Otherwise, if there is no stateless session associated with the
		 * current stream, a new stateless session will be created.
		 * </il>
		 * <p>
		 * The session will be closed automatically.
		 *
		 * @param tenantId the id of the tenant
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link CompletionStage}.
		 */
		<T> CompletionStage<T> withStatelessSession(String tenantId, Function<StatelessSession, CompletionStage<T>> work);

		/**
		 * @return an instance of {@link CriteriaBuilder} for creating
		 * criteria queries.
		 */
		CriteriaBuilder getCriteriaBuilder();

		/**
		 * Obtain the JPA {@link Metamodel} for the persistence unit.
		 */
		Metamodel getMetamodel();

		/**
		 * Obtain the {@link Cache} object for managing the second-level
		 * cache.
		 */
		Cache getCache();

		/**
		 * Obtain the {@link Statistics} object exposing factory-level
		 * metrics.
		 */
		Statistics getStatistics();

		/**
		 * Destroy the session factory and clean up its connection pool.
		 */
		@Override
		void close();

		/**
		 * @return false if {@link #close()} has been called
		 */
		boolean isOpen();
	}

	/**
	 * An object whose {@link #close()} method returns a {@link CompletionStage}.
	 */
	interface Closeable {
		CompletionStage<Void> close();
	}

	/**
	 * Asynchronously fetch an association that's configured for lazy loading.
	 *
	 * <pre>
	 * {@code Stage.fetch(author.getBook()).thenAccept(book -> print(book.getTitle()));}
	 * </pre>
	 *
	 * @param association a lazy-loaded association
	 *
	 * @return the fetched association, via a {@code CompletionStage}
	 *
	 * @see org.hibernate.Hibernate#initialize(Object)
	 */
	static <T> CompletionStage<T> fetch(T association) {
		if ( association == null ) {
			return CompletionStages.nullFuture();
		}

		SharedSessionContractImplementor session;
		if ( association instanceof HibernateProxy) {
			session = ( (HibernateProxy) association ).getHibernateLazyInitializer().getSession();
		}
		else if ( association instanceof PersistentCollection) {
			session = ( (AbstractPersistentCollection) association ).getSession();
		}
		else if ( ManagedTypeHelper.isPersistentAttributeInterceptable( association ) ) {
			final PersistentAttributeInterceptable interceptable = ManagedTypeHelper.asPersistentAttributeInterceptable( association );
			final PersistentAttributeInterceptor interceptor = interceptable.$$_hibernate_getInterceptor();
			if ( interceptor instanceof EnhancementAsProxyLazinessInterceptor) {
				session = ( (EnhancementAsProxyLazinessInterceptor) interceptor ).getLinkedSession();
			}
			else {
				return CompletionStages.completedFuture( association );
			}
		}
		else {
			return CompletionStages.completedFuture( association );
		}
		if ( session == null ) {
			throw LOG.sessionClosedLazyInitializationException();
		}
		return ReactiveQueryExecutorLookup.extract( session ).reactiveFetch( association, false );
	}
}
