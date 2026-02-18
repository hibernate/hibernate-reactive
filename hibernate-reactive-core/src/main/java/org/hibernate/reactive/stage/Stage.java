/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage;

import jakarta.persistence.TypedQueryReference;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.hibernate.Cache;
import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.bytecode.enhance.spi.interceptor.EnhancementAsProxyLazinessInterceptor;
import org.hibernate.collection.spi.AbstractPersistentCollection;
import org.hibernate.engine.spi.PersistentAttributeInterceptable;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.jpa.internal.util.FlushModeTypeHelper;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.query.Page;
import org.hibernate.query.criteria.HibernateCriteriaBuilder;
import org.hibernate.query.criteria.JpaCriteriaInsert;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.common.Identifier;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.impl.ReactiveQueryExecutorLookup;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.stat.Statistics;

import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Parameter;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.metamodel.Attribute;
import jakarta.persistence.metamodel.Metamodel;

import static org.hibernate.engine.internal.ManagedTypeHelper.asPersistentAttributeInterceptable;
import static org.hibernate.engine.internal.ManagedTypeHelper.isPersistentAttributeInterceptable;
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
	interface AbstractQuery {

		/**
		 * Set the value of an ordinal parameter. Ordinal parameters
		 * are numbered from 1, and are specified in the query using
		 * placeholder tokens of form {@code ?1}, {@code ?2}, etc.
		 *
		 * @param parameter an integer identifying the ordinal parameter
		 * @param argument the argument to set
		 */
		AbstractQuery setParameter(int parameter, Object argument);

		/**
		 * Set the value of a named parameter. Named parameters are
		 * specified in the query using placeholder tokens of form
		 * {@code :name}.
		 *
		 * @param parameter the name of the parameter
		 * @param argument the argument to set
		 */
		AbstractQuery setParameter(String parameter, Object argument);

		/**
		 * Set the value of a typed parameter. Typed parameters are
		 * obtained from the JPA {@link CriteriaBuilder}, which may
		 * itself be obtained by calling
		 * {@link SessionFactory#getCriteriaBuilder()}.
		 *
		 * @param parameter the parameter
		 * @param argument the argument to set
		 *
		 * @see CriteriaBuilder#parameter(Class)
		 */
		<T> AbstractQuery setParameter(Parameter<T> parameter, T argument);

		/**
		 * Set the comment for this query. This comment will be prepended
		 * to the SQL query sent to the database.
		 *
		 * @param comment The human-readable comment
		 */
		AbstractQuery setComment(String comment);

		String getComment();
	}

	interface SelectionQuery<R> extends AbstractQuery {

		/**
		 * Set the maximum number of results that may be returned by this
		 * query when executed.
		 */
		SelectionQuery<R> setMaxResults(int maxResults);

		/**
		 * Set the position of the first result that may be returned by
		 * this query when executed, where the results are numbered from
		 * 0.
		 */
		SelectionQuery<R> setFirstResult(int firstResult);

		/**
		 * Set the {@linkplain Page page} of results to return.
		 *
		 * @see Page
		 *
		 * @since 2.1
		 */
		@Incubating
		SelectionQuery<R> setPage(Page page);

		/**
		 * @return the maximum number results, or {@link Integer#MAX_VALUE}
		 *          if not set
		 */
		int getMaxResults();

		/**
		 * @return the first result, or 0 if not set
		 */
		int getFirstResult();

		/**
		 * Asynchronously execute this query, returning a single row that
		 * matches the query, throwing an exception if the query returns
		 * zero rows or more than one matching row. If the query has multiple
		 * results per row, the results are returned in an instance of
		 * {@code Object[]}.
		 *
		 * @return the single resulting row
		 * @throws jakarta.persistence.NoResultException if there is no result
		 * @throws jakarta.persistence.NonUniqueResultException if there are multiple results
		 *
		 * @see jakarta.persistence.Query#getSingleResult()
		 */
		CompletionStage<R> getSingleResult();

		/**
		 * Asynchronously execute this query, returning a single row that
		 * matches the query, or {@code null} if the query returns no results,
		 * throwing an exception if the query returns more than one matching
		 * row. If the query has multiple results per row, the results are
		 * returned in an instance of {@code Object[]}.
		 *
		 * @return the single resulting row or {@code null}
		 * @throws jakarta.persistence.NonUniqueResultException if there are multiple results
		 *
		 * @see #getSingleResult()
		 */
		CompletionStage<R> getSingleResultOrNull();

		/**
		 * Determine the size of the query result list that would be
		 * returned by calling {@link #getResultList()} with no
		 * {@linkplain #getFirstResult() offset} or
		 * {@linkplain #getMaxResults() limit} applied to the query.
		 *
		 * @return the size of the list that would be returned
		 */
		@Incubating
		CompletionStage<Long> getResultCount();

		/**
		 * Asynchronously execute this query, returning the query results
		 * as a {@link List}, via a {@link CompletionStage}. If the query
		 * has multiple results per row, the results are returned in an
		 * instance of {@code Object[]}.
		 *
		 * @return the resulting rows as a {@link List}
		 *
		 * @see jakarta.persistence.Query#getResultList()
		 */
		CompletionStage<List<R>> getResultList();

		/**
		 * Set the read-only/modifiable mode for entities and proxies
		 * loaded by this Query. This setting overrides the default setting
		 * for the persistence context.
		 *
		 * @see Session#setDefaultReadOnly(boolean)
		 */
		SelectionQuery<R> setReadOnly(boolean readOnly);

		/**
		 * @return the read-only/modifiable mode
		 *
		 * @see Session#isDefaultReadOnly()
		 */
		boolean isReadOnly();

		/**
		 * Enable or disable caching of this query result set in the
		 * second-level query cache.
		 *
		 * @param cacheable {@code true} if this query is cacheable
		 */
		SelectionQuery<R> setCacheable(boolean cacheable);

		/**
		 * @return {@code true} if this query is cacheable
		 *
		 * @see #setCacheable(boolean)
		 */
		boolean isCacheable();

		/**
		 * Set the name of the cache region in which to store this
		 * query result set if {@link #setCacheable(boolean)
		 * caching is enabled}.
		 *
		 * @param cacheRegion the name of the cache region
		 */
		SelectionQuery<R> setCacheRegion(String cacheRegion);

		/**
		 * @return the name of the cache region
		 *
		 * @see #setCacheRegion(String)
		 */
		String getCacheRegion();

		/**
		 * Set the current {@link CacheMode} in effect while this query
		 * is being executed.
		 */
		SelectionQuery<R> setCacheMode(CacheMode cacheMode);

		/**
		 * Set the current {@link CacheStoreMode} in effect while this query
		 * is being executed.
		 */
		default SelectionQuery<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
			return setCacheMode( interpretCacheMode( cacheStoreMode, interpretCacheRetrieveMode( getCacheMode() ) ) );
		}

		/**
		 * Set the current {@link CacheRetrieveMode} in effect while this query
		 * is being executed.
		 */
		default SelectionQuery<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
			return setCacheMode( interpretCacheMode( interpretCacheStoreMode( getCacheMode() ), cacheRetrieveMode ) );
		}

		CacheStoreMode getCacheStoreMode();

		CacheRetrieveMode getCacheRetrieveMode();

		/**
		 * Obtain the {@link CacheMode} in effect for this query. By default,
		 * the query inherits the {@code CacheMode} of the {@link Session}
		 * from which is originates.
		 *
		 * @see Session#getCacheMode()
		 */
		CacheMode getCacheMode();

		/**
		 * Set the current {@link FlushMode} in effect while this query is
		 * being executed.
		 */
		SelectionQuery<R> setFlushMode(FlushMode flushMode);

		/**
		 * Set the current {@link FlushModeType} in effect while this query is
		 * being executed.
		 */
		default SelectionQuery<R> setFlushMode(FlushModeType flushModeType) {
			return setFlushMode( FlushModeTypeHelper.getFlushMode(flushModeType) );
		}

		/**
		 * Obtain the {@link FlushMode} in effect for this query. By default,
		 * the query inherits the {@code FlushMode} of the {@link Session}
		 * from which is originates.
		 *
		 * @see Session#getFlushMode()
		 */
		FlushMode getFlushMode();

		/**
		 * Set the {@link LockMode} to use for the whole query.
		 */
		SelectionQuery<R> setLockMode(LockMode lockMode);

		/**
		 * Set the {@link LockModeType} to use for the whole query.
		 */
		default SelectionQuery<R> setLockMode(LockModeType lockModeType) {
			return setLockMode( convertToLockMode(lockModeType) );
		}

		/**
		 * Set the {@link LockMode} to use for specified alias (as defined in
		 * the query's {@code from} clause).
		 *
		 * @param alias the from clause alias
		 * @param lockMode the requested {@link LockMode}
		 *
		 * @see org.hibernate.query.Query#setLockMode(String,LockMode)
		 */
		SelectionQuery<R> setLockMode(String alias, LockMode lockMode);

		/**
		 * Set the {@link LockModeType} to use for specified alias (as defined in
		 * the query's {@code from} clause).
		 *
		 * @param alias the from clause alias
		 * @param lockModeType the requested {@link LockModeType}
		 *
		 * @see org.hibernate.query.Query#setLockMode(String,LockMode)
		 */
		default SelectionQuery<R> setLockMode(String alias, LockModeType lockModeType) {
			return setLockMode( alias, convertToLockMode(lockModeType) );
		}

		/**
		 * Set the {@link EntityGraph} that will be used as a fetch plan for
		 * the root entity returned by this query.
		 */
		SelectionQuery<R> setPlan(EntityGraph<R> entityGraph);

		/**
		 * Enable a {@linkplain org.hibernate.annotations.FetchProfile fetch
		 * profile} which will be in effect during execution of this query.
		 */
		SelectionQuery<R> enableFetchProfile(String profileName);

		@Override
		SelectionQuery<R> setParameter(int parameter, Object argument);

		@Override
		SelectionQuery<R> setParameter(String parameter, Object argument);

		@Override
		<T> SelectionQuery<R> setParameter(Parameter<T> parameter, T argument);

		@Override
		SelectionQuery<R> setComment(String comment);
	}

	interface MutationQuery extends AbstractQuery {
		/**
		 * Asynchronously execute this delete, update, or insert query,
		 * returning the updated row count.
		 *
		 * @return the row count as an integer
		 *
		 * @see jakarta.persistence.Query#executeUpdate()
		 */
		CompletionStage<Integer> executeUpdate();

		@Override
		MutationQuery setParameter(int parameter, Object argument);

		@Override
		MutationQuery setParameter(String parameter, Object argument);

		@Override
		<T> MutationQuery setParameter(Parameter<T> parameter, T argument);

		@Override
		MutationQuery setComment(String comment);
	}

	interface Query<R> extends SelectionQuery<R>, MutationQuery {
		@Override
		Query<R> setMaxResults(int maxResults);

		@Override
		Query<R> setFirstResult(int firstResult);

		@Override
		Query<R> setReadOnly(boolean readOnly);

		@Override
		Query<R> setCacheable(boolean cacheable);

		@Override
		Query<R> setCacheRegion(String cacheRegion);

		@Override
		Query<R> setCacheMode(CacheMode cacheMode);

		@Override
		default Query<R> setCacheStoreMode(CacheStoreMode cacheStoreMode) {
			SelectionQuery.super.setCacheStoreMode( cacheStoreMode );
			return this;
		}

		@Override
		default Query<R> setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
			SelectionQuery.super.setCacheRetrieveMode( cacheRetrieveMode );
			return this;
		}

		@Override
		Query<R> setFlushMode(FlushMode flushMode);

		@Override
		default Query<R> setFlushMode(FlushModeType flushModeType) {
			SelectionQuery.super.setFlushMode( flushModeType );
			return this;
		}

		@Override
		Query<R> setLockMode(LockMode lockMode);

		@Override
		default Query<R> setLockMode(LockModeType lockModeType) {
			SelectionQuery.super.setLockMode( lockModeType );
			return this;
		}

		@Override
		Query<R> setLockMode(String alias, LockMode lockMode);

		@Override
		default Query<R> setLockMode(String alias, LockModeType lockModeType) {
			SelectionQuery.super.setLockMode( alias, lockModeType );
			return this;
		}

		@Override
		Query<R> setPlan(EntityGraph<R> entityGraph);

		@Override
		Query<R> setParameter(int parameter, Object argument);

		@Override
		Query<R> setParameter(String parameter, Object argument);

		@Override
		<T> Query<R> setParameter(Parameter<T> parameter, T argument);

		@Override
		Query<R> setComment(String comment);
	}

	/**
	 * Operations common to objects which act as factories for instances of
	 * {@link Query}. This is a common supertype of {@link Session} and
	 * {@link StatelessSession}.
	 *
	 * @since 3.0
	 */
	sealed interface QueryProducer extends Closeable permits Session, StatelessSession {
		/**
		 * Create an instance of {@link SelectionQuery} for the given HQL/JPQL
		 * query string.
		 *
		 * @param queryString The HQL/JPQL query
		 *
		 * @return The {@link SelectionQuery} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String, Class)
		 */
		<R> SelectionQuery<R> createSelectionQuery(String queryString, Class<R> resultType);

		/**
		 * Create an instance of {@link MutationQuery} for the given HQL/JPQL
		 * update or delete statement.
		 *
		 * @param queryString The HQL/JPQL query, update or delete statement
		 *
		 * @return The {@link MutationQuery} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String)
		 */
		MutationQuery createMutationQuery(String queryString);

		/**
		 * Create an instance of {@link MutationQuery} for the given update tree.
		 *
		 * @param updateQuery the update criteria query
		 *
		 * @return The {@link MutationQuery} instance for manipulation and execution
		 *
		 * @see org.hibernate.query.QueryProducer#createMutationQuery(CriteriaUpdate)
		 */
		MutationQuery createMutationQuery(CriteriaUpdate<?> updateQuery);

		/**
		 * Create an instance of {@link MutationQuery} for the given delete tree.
		 *
		 * @param deleteQuery the delete criteria query
		 *
		 * @return The {@link MutationQuery} instance for manipulation and execution
		 *
		 * @see org.hibernate.query.QueryProducer#createMutationQuery(CriteriaDelete)
		 */
		MutationQuery createMutationQuery(CriteriaDelete<?> deleteQuery);

		/**
		 * Create a {@link MutationQuery} from the given insert select criteria tree
		 *
		 * @param insert the insert select criteria query
		 *
		 * @return The {@link MutationQuery} instance for manipulation and execution
		 *
		 * @see org.hibernate.query.QueryProducer#createMutationQuery(JpaCriteriaInsert)
		 */
		MutationQuery createMutationQuery(JpaCriteriaInsert<?> insert);

		/**
		 * Create a typed {@link Query} instance for the given typed query reference.
		 *
		 * @param typedQueryReference the type query reference
		 *
		 * @return The {@link Query} instance for execution
		 *
		 * @throws IllegalArgumentException if a query has not been
		 * defined with the name of the typed query reference or if
		 * the query result is found to not be assignable to
		 * result class of the typed query reference
		 *
		 * @see org.hibernate.query.QueryProducer#createQuery(TypedQueryReference)
		 */
		<R> Query<R> createQuery(TypedQueryReference<R> typedQueryReference);

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
		 * @deprecated See explanation in
		 * {@link org.hibernate.query.QueryProducer#createSelectionQuery(String)}
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String)
		 */
		@Deprecated
		<R> Query<R> createQuery(String queryString);

		/**
		 * Create an instance of {@link SelectionQuery} for the given HQL/JPQL
		 * query string and query result type.
		 *
		 * @param queryString The HQL/JPQL query
		 * @param resultType the Java type returned in each row of query results
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String, Class)
		 */
		<R> SelectionQuery<R> createQuery(String queryString, Class<R> resultType);

		/**
		 * Create an instance of {@link MutationQuery} for the given criteria
		 * update.
		 *
		 * @param criteriaUpdate The {@link CriteriaUpdate}
		 *
		 * @return The {@link MutationQuery} instance for manipulation and execution
		 */
		<R> MutationQuery createQuery(CriteriaUpdate<R> criteriaUpdate);

		/**
		 * Create an instance of {@link MutationQuery} for the given criteria
		 * delete.
		 *
		 * @param criteriaDelete The {@link CriteriaDelete}
		 *
		 * @return The {@link MutationQuery} instance for manipulation and execution
		 */
		<R> MutationQuery createQuery(CriteriaDelete<R> criteriaDelete);

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
		 * Create an instance of {@link SelectionQuery} for the named query.
		 *
		 * @param queryName The name of the query
		 * @param resultType the Java type returned in each row of query results
		 *
		 * @return The {@link SelectionQuery} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String, Class)
		 */
		<R> SelectionQuery<R> createNamedQuery(String queryName, Class<R> resultType);

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
		 * @return The {@link SelectionQuery} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createNativeQuery(String, Class)
		 */
		<R> SelectionQuery<R> createNativeQuery(String queryString, Class<R> resultType);

		/**
		 * Create an instance of {@link SelectionQuery} for the given SQL query
		 * string, using the given {@code resultType} to interpret the results.
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
		 * @see jakarta.persistence.EntityManager#createNativeQuery(String, Class)
		 */
		<R> SelectionQuery<R> createNativeQuery(String queryString, Class<R> resultType, AffectedEntities affectedEntities);

		/**
		 * Create an instance of {@link SelectionQuery} for the given SQL query
		 * string, using the given {@link ResultSetMapping} to interpret the
		 * result set.
		 *
		 * @param queryString The SQL query
		 * @param resultSetMapping the result set mapping
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see #getResultSetMapping(Class, String)
		 * @see jakarta.persistence.EntityManager#createNativeQuery(String, String)
		 */
		<R> SelectionQuery<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping);

		/**
		 * Create an instance of {@link SelectionQuery} for the given SQL query
		 * string, using the given {@link ResultSetMapping} to interpret the
		 * result set.
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
		<R> SelectionQuery<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping, AffectedEntities affectedEntities);

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
		 * Create an instance of {@link SelectionQuery} for the given criteria
		 * query.
		 *
		 * @param criteriaQuery The {@link CriteriaQuery}
		 *
		 * @return The {@link SelectionQuery} instance for manipulation and execution
		 *
		 * @see jakarta.persistence.EntityManager#createQuery(String)
		 */
		<R> SelectionQuery<R> createQuery(CriteriaQuery<R> criteriaQuery);

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
		 * Convenience method to obtain the {@link CriteriaBuilder}.
		 *
		 * @since 3
		 */
		CriteriaBuilder getCriteriaBuilder();
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
    non-sealed interface Session extends QueryProducer {

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
		 * Make a transient instance persistent and mark it for later insertion in the
		 * database. This operation cascades to associated instances if the association
		 * is mapped with {@link jakarta.persistence.CascadeType#PERSIST}.
		 * <p>
		 * For entities with a {@link jakarta.persistence.GeneratedValue generated id},
		 * {@code persist()} ultimately results in generation of an identifier for the
		 * given instance. But this may happen asynchronously, when the session is
		 * {@linkplain #flush() flushed}, depending on the identifier generation strategy.
		 *
		 * @param entityName the entity name
		 * @param object a transient instance to be made persistent
		 * @see #persist(Object)
		 */
		CompletionStage<Void> persist(String entityName, Object object);

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
		CompletionStage<Void> merge(Object... entities);

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
		 * Asynchronously fetch an association configured for lazy loading.
		 * <p>
		 * <pre>
		 * {@code session.fetch(author.getBook()).thenAccept(book -> print(book.getTitle()))}
		 * </pre>
		 * </p>
		 * <p>
		 * This operation may be even be used to initialize a reference returned by
		 * {@link #getReference(Class, Object)}.
		 * <p>
		 * <pre>
		 * {@code session.fetch(session.getReference(Author.class, authorId))}
		 * </pre>
		 * </p>
		 *
		 * @param association a lazy-loaded association, or a proxy
		 *
		 * @return the fetched association, via a {@code CompletionStage}
		 *
		 * @see Stage#fetch(Object)
		 * @see #getReference(Class, Object)
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
		 * Get the maximum batch size for batch fetching associations by
		 * id in this session.
		 *
		 * @since 2.1
		 */
		int getFetchBatchSize();

		/**
		 * Set the maximum batch size for batch fetching associations by
		 * id in this session.
		 * Override the default controlled by the configuration property
		 * {@code hibernate.default_batch_fetch_size}.
		 * <p>
		 * <ul>
		 * <li>If {@code batchSize>1}, then batch fetching is enabled.
		 * <li>If {@code batchSize<0}, the batch size is inherited from
		 *     the factory-level setting.
		 * <li>Otherwise, batch fetching is disabled.
		 * </ul>
		 *
		 * @param batchSize the maximum batch size for batch fetching
		 *
		 * @since 2.1
		 */
		Stage.Session setFetchBatchSize(int batchSize);

		/**
		 * Determine if subselect fetching is enabled in this session.
		 *
		 * @return {@code true} if subselect fetching is enabled
		 *
		 * @since 2.1
		 */
		boolean isSubselectFetchingEnabled();

		/**
		 * Enable or disable subselect fetching in this session.
		 * Override the default controlled by the configuration property
		 * {@code hibernate.use_subselect_fetch}.
		 *
		 * @param enabled {@code true} to enable subselect fetching
		 *
		 * @since 2.1
		 */
		Stage.Session setSubselectFetchingEnabled(boolean enabled);

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
		 * The {@link SessionFactory} which created this session.
		 */
		SessionFactory getFactory();
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
    non-sealed interface StatelessSession extends QueryProducer {

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
		 * Retrieve multiple rows.
		 *
		 * @param entityClass The class of the entity to retrieve
		 * @param ids The ids of the entities to retrieve
		 *
		 * @return a list of detached entity instances, via a {@code Uni}
		 *
		 * @see org.hibernate.StatelessSession#getMultiple(Class, List)
		 */
		<T> CompletionStage<List<T>> get(Class<T> entityClass, Object... ids);

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
		 * Insert a row.
		 *
		 * @param entity a new transient instance
		 *
		 * @see org.hibernate.StatelessSession#insert(Object)
		 */
		CompletionStage<Void> insert(Object entity);

		/**
		 * Insert multiple rows, using the number of the
		 * given entities as the batch size.
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
		 * Insert multiple rows, using the size of the
		 * given list as the batch size.
		 *
		 * @param entities new transient instances
		 *
		 * @see org.hibernate.StatelessSession#insert(Object)
		 */
		CompletionStage<Void> insertMultiple(List<?> entities);

		/**
		 * Delete a row.
		 *
		 * @param entity a detached entity instance
		 *
		 * @see org.hibernate.StatelessSession#delete(Object)
		 */
		CompletionStage<Void> delete(Object entity);

		/**
		 * Delete multiple rows, using the number of the
		 * given entities as the batch size.
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
		 * Delete multiple rows, using the size of the
		 * given list as the batch size.
		 *
		 * @param entities detached entity instances
		 *
		 * @see org.hibernate.StatelessSession#delete(Object)
		 */
		CompletionStage<Void> deleteMultiple(List<?> entities);

		/**
		 * Update a row.
		 *
		 * @param entity a detached entity instance
		 *
		 * @see org.hibernate.StatelessSession#update(Object)
		 */
		CompletionStage<Void> update(Object entity);

		/**
		 * Update multiple rows, using the number of the
		 * given entities as the batch size.
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
		 * Update multiple rows, using the size of the
		 * given list as the batch size.
		 *
		 * @param entities a detached entity instance
		 *
		 * @see org.hibernate.StatelessSession#update(Object)
		 */
		CompletionStage<Void> updateMultiple(List<?> entities);

		/**
		 * Refresh the entity instance state from the database.
		 *
		 * @param entity The entity to be refreshed.
		 *
		 * @see org.hibernate.StatelessSession#refresh(Object)
		 */
		CompletionStage<Void> refresh(Object entity);

		/**
		 * Refresh the entity instance state from the database, using the number of the
		 * given entities as the batch size.
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
		 * Refresh the entity instance state from the database,
		 * using the size of the given list as the batch size.
		 *
		 * @param entities The entities to be refreshed.
		 *
		 * @see org.hibernate.StatelessSession#refresh(Object)
		 */
		CompletionStage<Void> refreshMultiple(List<?> entities);

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
		 *
		 * @param entity a detached entity instance
		 *
		 * @see org.hibernate.StatelessSession#upsert(Object)
		 */
		CompletionStage<Void> upsert(Object entity);

		/**
		 * Use a SQL {@code merge into} statement to perform
		 * an upsert on multiple rows using the size of the given array
		 * as batch size.
		 *
		 * @param entities the entities to upsert
		 *
		 * @see org.hibernate.StatelessSession#upsert(Object)
		 */
		@Incubating
		CompletionStage<Void> upsert(Object... entities);

		/**
		 * Use a SQL {@code merge into} statement to perform
		 * an upsert on multiple rows using the specified batch size.
		 *
		 * @param batchSize the batch size
		 * @param entities the list of entities to upsert
		 *
		 * @see org.hibernate.StatelessSession#upsert(Object)
		 */
		@Incubating
		CompletionStage<Void> upsert(int batchSize, Object... entities);

		/**
		 * Use a SQL {@code merge into} statement to perform
		 * an upsert on multiple rows using the size of the given array
		 * as batch size.
		 *
		 * @param entities the entities to upsert
		 *
		 * @see org.hibernate.StatelessSession#upsert(Object)
		 *
		 * @deprecated Use {@link #upsert(Object...)} instead
		 */
		@Incubating @Deprecated(forRemoval = true)
		CompletionStage<Void> upsertAll(Object... entities);

		/**
		 * Use a SQL {@code merge into} statement to perform
		 * an upsert on multiple rows using the specified batch size.
		 *
		 * @param batchSize the batch size
		 * @param entities the list of entities to upsert
		 *
		 * @see org.hibernate.StatelessSession#upsert(Object)
		 *
		 * @deprecated Use {@link #upsert(int, Object...)} instead
		 */
		@Incubating @Deprecated(forRemoval = true)
		CompletionStage<Void> upsertAll(int batchSize, Object... entities);

		/**
		 * Use a SQL {@code merge into} statement to perform
		 * an upsert on multiple rows using the size of the given list
		 * as batch size.
		 *
		 * @param entities the entities to upsert
		 *
		 * @see org.hibernate.StatelessSession#upsert(Object)
		 */
		@Incubating
		CompletionStage<Void> upsertMultiple(List<?> entities);

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
		 * Return the identifier value of the given entity, which may be detached.
		 *
		 * @param entity a persistent instance associated with this session
		 *
		 * @return the identifier
		 *
		 * @since 3.0
		 */
		Object getIdentifier(Object entity);

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
		 * The {@link SessionFactory} which created this session.
		 */
		SessionFactory getFactory();

		/**
		 * Convenience method to obtain the {@link CriteriaBuilder}.
		 *
		 * @since 3
		 */
		default CriteriaBuilder getCriteriaBuilder() {
			return getFactory().getCriteriaBuilder();
		}
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
		 * Obtain a new {@link Session reactive session}.
		 * <p>
		 * The underlying database connection is obtained lazily
		 * when the returned {@link Session} needs to access the
		 * database.
		 * <p>
		 * The client must close the session using {@link Session#close()}.
		 */
		@Incubating
		Session createSession();

		/**
		 * Obtain a new {@link Session reactive session}.
		 * <p>
		 * The underlying database connection is obtained lazily
		 * when the returned {@link Session} needs to access the
		 * database.
		 * <p>
		 * The client must close the session using {@link Session#close()}.
		 */
		@Incubating
		Session createSession(String tenantId);

		/**
		 * Obtain a new {@link Session reactive session}.
		 * <p>
		 * The underlying database connection is obtained lazily
		 * when the returned {@link Session} needs to access the
		 * database.
		 * <p>
		 * The client must close the session using {@link Session#close()}.
		 */
		@Incubating
		StatelessSession createStatelessSession();

		/**
		 * Obtain a new {@link StatelessSession reactive stateless session}.
		 * <p>
		 * The underlying database connection is obtained lazily
		 * when the returned {@link StatelessSession} needs to access the
		 * database.
		 * <p>
		 * The client must close the session using {@link Session#close()}.
		 * @param tenantId the id of the tenant
		 */
		@Incubating
		StatelessSession createStatelessSession(String tenantId);

		/**
		 * Obtain a new {@linkplain Session reactive session} {@link CompletionStage}, the main
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
		 * Obtain a new {@linkplain Session reactive session} {@link CompletionStage} for a
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
		 * Perform work using a {@linkplain Session reactive session}.
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
		 * Perform work using a {@linkplain Session reactive session} for a
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
		 * Perform work using a {@linkplain Session reactive session} within an
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
		 * Perform work using a {@linkplain Session reactive session} within an
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
		 * Perform work using a {@linkplain Session reactive session} for
		 * the tenant with the specified tenant id within an associated
		 * {@link Transaction transaction}.
		 * <p>
		 * <il>
		 * <li>If there is already a session associated with the current
		 * reactive stream and given tenant id, then the work will be
		 * executed using that session.
		 * <li>Otherwise, if there is no stateless session associated with
		 * the current stream and given tenant id, a new stateless session
		 * will be created.
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
		 * Perform work using a {@linkplain StatelessSession reactive session}
		 * within an associated {@link Transaction transaction}.
		 * <p>
		 * <il>
		 * <li>If there is already a stateless session associated with the
		 * current reactive stream, then the work will be executed using that
		 * session.
		 * <li>Otherwise, if there is no stateless session associated with the
		 * current stream, a new stateless session will be created.
		 * </il>
		 * <p>
		 * The session will be closed automatically, and the transaction committed
		 * automatically.
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
		 * Perform work using a {@linkplain StatelessSession reactive session}
		 * within an associated {@link Transaction transaction}.
		 * <p>
		 * <il>
		 * <li>If there is already a stateless session associated with the
		 * current reactive stream, then the work will be executed using that
		 * session.
		 * <li>Otherwise, if there is no stateless session associated with the
		 * current stream, a new stateless session will be created.
		 * </il>
		 * <p>
		 * The session will be closed automatically, and the transaction committed
		 * automatically.
		 *
		 * @param work a function which accepts the stateless session and returns
		 *             the result of the work as a {@link CompletionStage}.
		 *
		 * @see #withStatelessSession(Function)
		 * @see StatelessSession#withTransaction(Function)
		 */
		<T> CompletionStage<T> withStatelessTransaction(BiFunction<StatelessSession, Transaction, CompletionStage<T>> work);

		/**
		 * Perform work using a {@linkplain StatelessSession reactive session}
		 * for the tenant with the specified tenant id within an associated
		 * {@link Transaction transaction}.
		 * <p>
		 * <il>
		 * <li>If there is already a stateless session associated with the
		 * current reactive stream and given tenant id, then the work will be
		 * executed using that session.
		 * <li>Otherwise, if there is no stateless session associated with the
		 * current stream and given tenant id, a new stateless session will be
		 * created.
		 * </il>
		 * <p>
		 * The session will be closed automatically and the transaction committed
		 * automatically.
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
		 * Perform work using a {@linkplain StatelessSession stateless session}.
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
		 * Perform work using a {@linkplain StatelessSession stateless session}.
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
		HibernateCriteriaBuilder getCriteriaBuilder();

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
		 * Return the current instance of {@link Session}, if any.
		 * A current session exists only when this method is called
		 * from within an invocation of {@link #withSession(Function)}
		 * or {@link #withTransaction(Function)}.
		 *
		 * @return the current instance, if any, or {@code null}
		 *
		 * @since 3.0
		 */
		Session getCurrentSession();

		/**
		 * Return the current instance of {@link Session}, if any.
		 * A current session exists only when this method is called
		 * from within an invocation of
		 * {@link #withStatelessSession(Function)} or
		 * {@link #withStatelessTransaction(Function)}.
		 *
		 * @return the current instance, if any, or {@code null}
		 *
		 * @since 3.0
		 */
		StatelessSession getCurrentStatelessSession();

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
		/**
		 * Destroy the object and release any underlying database resources.
		 */
		CompletionStage<Void> close();
		/**
		 * @return false if {@link #close()} has been called
		 */
		boolean isOpen();
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

		final SharedSessionContractImplementor session;
		if ( association instanceof HibernateProxy proxy ) {
			session = proxy.getHibernateLazyInitializer().getSession();
		}
		else if ( association instanceof AbstractPersistentCollection<?> collection ) {
			session = collection.getSession();
		}
		else if ( isPersistentAttributeInterceptable( association ) ) {
			final PersistentAttributeInterceptable interceptable = asPersistentAttributeInterceptable( association );
			final PersistentAttributeInterceptor interceptor = interceptable.$$_hibernate_getInterceptor();
			if ( interceptor instanceof EnhancementAsProxyLazinessInterceptor lazinessInterceptor) {
				session = lazinessInterceptor.getLinkedSession();
			}
			else {
				return CompletionStages.completedFuture( association );
			}
		}
		else {
			return CompletionStages.completedFuture( association );
		}
		if ( session == null ) {
			throw LoggerFactory.make( Log.class, MethodHandles.lookup() ).sessionClosedLazyInitializationException();
		}
		return ReactiveQueryExecutorLookup.extract( session ).reactiveFetch( association, false );
	}
}
