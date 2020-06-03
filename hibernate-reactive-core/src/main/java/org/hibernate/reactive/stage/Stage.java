/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.stage;

import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.LazyInitializationException;
import org.hibernate.LockMode;
import org.hibernate.collection.internal.AbstractPersistentCollection;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;

import javax.persistence.EntityGraph;
import javax.persistence.Parameter;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.Metamodel;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

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
	 * Note that {@link javax.persistence.TemporalType} is not supported
	 * as an argument for parameter bindings, and so parameters of type
	 * {@link java.util.Date} or {@link java.util.Calendar} should not be
	 * used. Instead, datetime types from {@code java.time} should be used
	 * as parameters.
	 *
	 * @see javax.persistence.Query
	 */
	interface Query<R> {

		/**
		 * Set the value of a positional parameter. Positional parameters
		 * are numbered from 1, and are specified in the query using
		 * placeholder tokens of form {@code ?1}, {@code ?2}, etc.
		 */
		Query<R> setParameter(int position, Object value);

		/**
		 * Set the value of a named parameter. Named parameters are
		 * specified in the query using placeholder tokens of form
		 * {@code :name}.
		 */
		Query<R> setParameter(String name, Object value);

		/**
		 * Set the value of a typed parameter. Typed parameters are
		 * obtained from the JPA {@link CriteriaBuilder}, which may
		 * itself be obtained by calling
		 * {@link SessionFactory#getCriteriaBuilder()}.
		 *
		 * @see CriteriaBuilder#parameter(Class)
		 */
		<T> Query<R> setParameter(Parameter<T> name, T value);

		/**
		 * Set the maximum number of results that may be returned by this
		 * query when executed.
		 */
		Query<R> setMaxResults(int maxResults);

		/**
		 * Set the position of the first result that may be returned by
		 * this query when executed, where the results are numbered from
		 * 0.
		 */
		Query<R> setFirstResult(int firstResult);

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
		 * Asynchronously Execute this query, returning a single row
		 * that matches the query, or {@code null} if the query returns
		 * no results, throwing an exception if the query returns more
		 * than one matching row. If the query has multiple results per
		 * row, the results are returned in an instance of {@code Object[]}.
		 *
		 * @return the single resulting row or {@code null}
		 *
		 * @see javax.persistence.Query#getSingleResult()
		 */
		CompletionStage<R> getSingleResult();

		/**
		 * Asynchronously execute this query, returning the query results
		 * as a {@link List}, via a {@link CompletionStage}. If the query
		 * has multiple results per row, the results are returned in an
		 * instance of {@code Object[]}.
		 *
		 * @return the resulting rows as a {@link List}
		 *
		 * @see javax.persistence.Query#getResultList()
		 */
		CompletionStage<List<R>> getResultList();

		/**
		 * Asynchronously execute this delete, update, or insert query,
		 * returning the updated row count.
		 *
		 * @return the row count as an integer
		 *
		 * @see javax.persistence.Query#executeUpdate()
		 */
		CompletionStage<Integer> executeUpdate();

		/**
		 * Set the read-only/modifiable mode for entities and proxies
		 * loaded by this Query. This setting overrides the default setting
		 * for the persistence context.
		 *
		 * @see Session#setDefaultReadOnly(boolean)
		 */
		Query<R> setReadOnly(boolean readOnly);

		/**
		 * @return the read-only/modifiable mode
		 *
		 * @see Session#isDefaultReadOnly()
		 */
		boolean isReadOnly();

		/**
		 * Set the comment for this query. This comment will be prepended
		 * to the SQL query sent to the database.
		 *
		 * @param comment The human-readable comment
		 */
		Query<R> setComment(String comment);

//		/**
//		 * Set a query hint.
//		 */
//		Query<R> setHint(String hintName, Object value);

		/**
		 * Set the current {@link CacheMode} in effect while this query
		 * is being executed.
		 */
		Query<R> setCacheMode(CacheMode cacheMode);

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
		Query<R> setFlushMode(FlushMode flushMode);

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
		Query<R> setLockMode(LockMode lockMode);

		/**
		 * Set the {@link LockMode} to use for specified alias (as defined in
		 * the query's {@code from} clause).
		 *
		 * @see org.hibernate.query.Query#setLockMode(String,LockMode)
		 */
		Query<R> setLockMode(String alias, LockMode lockMode);

//		/**
//		 * Set the {@link EntityGraph} that will be used as a fetch plan for
//		 * the root entity returned by this query.
//		 */
//		Query<R> setPlan(EntityGraph<R> entityGraph);

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
	interface Session extends AutoCloseable {

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
		 * @see javax.persistence.EntityManager#find(Class, Object)
		 */
		<T> CompletionStage<T> find(Class<T> entityClass, Object id);

		/**
		 * Asynchronously return the persistent instance of the given entity
		 * class with the given identifier, requesting the given {@link LockMode}.
		 *
		 * @see #find(Class,Object)
		 */
		<T> CompletionStage<T> find(Class<T> entityClass, Object id, LockMode lockMode);

		/**
		 * Asynchronously return the persistent instance of the given entity
		 * class with the given identifier, using the given {@link EntityGraph}
		 * as a fetch plan.
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
		 * @return a list of persistent instances and nulls via a {@code CompletionStage}
		 */
		<T> CompletionStage<List<T>> find(Class<T> entityClass, Object... ids);

		/**
		 * Return the persistent instance of the given entity class with the
		 * given identifier, assuming that the instance exists. This method
		 * never results in access to the underlying data store, and thus
		 * might return a proxied instance that must be initialized explicitly
		 * using {@link #fetch(Object)}.
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
		 * @see javax.persistence.EntityManager#getReference(Class, Object)
		 */
		<T> T getReference(Class<T> entityClass, Object id);

		/**
		 * Asynchronously persist the given transient instance, first assigning
		 * a generated identifier. (Or using the current value of the identifier
		 * property if the entity has assigned identifiers.)
		 * <p>
		 * This operation cascades to associated instances if the association is
		 * mapped with {@link javax.persistence.CascadeType#PERSIST}.
		 *
		 * <pre>
		 * {@code session.persist(newBook).thenAccept(v -> session.flush());}
		 * </pre>
		 *
		 * @param entity a transient instance of a persistent class
		 *
		 * @see javax.persistence.EntityManager#persist(Object)
		 */
		CompletionStage<Session> persist(Object entity);

		/**
		 * Persist multiple transient entity instances at once.
		 *
		 * @see #persist(Object)
		 */
		CompletionStage<Session> persist(Object... entities);

		/**
		 * Asynchronously remove a persistent instance from the datastore. The
		 * argument may be an instance associated with the receiving session or
		 * a transient instance with an identifier associated with existing
		 * persistent state.
		 * <p>
		 * This operation cascades to associated instances if the association is
		 * mapped with {@link javax.persistence.CascadeType#REMOVE}.
		 *
		 * <pre>
		 * {@code session.delete(book).thenAccept(v -> session.flush());}
		 * </pre>
		 *
		 * @param entity the instance to be removed
		 *
		 * @see javax.persistence.EntityManager#remove(Object)
		 */
		CompletionStage<Session> remove(Object entity);

		/**
		 * Remove multiple entity instances at once.
		 *
		 * @see #remove(Object)
		 */
		CompletionStage<Session> remove(Object... entities);

		/**
		 * Copy the state of the given object onto the persistent instance with
		 * the same identifier. If there is no such persistent instance currently
		 * associated with the session, it will be loaded. Return the persistent
		 * instance. Or, if the given instance is transient, save a copy of it
		 * and return the copy as a newly persistent instance. The given instance
		 * does not become associated with the session.
		 * <p>
		 * This operation cascades to associated instances if the association is
		 * mapped with {@link javax.persistence.CascadeType#MERGE}.
		 *
		 * @param entity a detached instance with state to be copied
		 *
		 * @return an updated persistent instance
		 *
		 * @see javax.persistence.EntityManager#merge(Object)
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
		 * @param entity a persistent or detached instance
		 *
		 * @see javax.persistence.EntityManager#refresh(Object)
		 */
		CompletionStage<Session> refresh(Object entity);

		/**
		 * Re-read the state of the given instance from the underlying database,
		 * requesting the given {@link LockMode}.
		 *
		 * @see #refresh(Object)
		 */
		CompletionStage<Session> refresh(Object entity, LockMode lockMode);

		/**
		 * Refresh multiple entity instances at once.
		 *
		 * @see #refresh(Object)
		 */
		CompletionStage<Session> refresh(Object... entities);

		/**
		 * Obtain the specified lock level upon the given object. For example,
		 * this operation may be used to:
		 *
		 * <ul>
		 * <li>perform a version check with {@link LockMode#READ}, or
		 * <li>upgrade to a pessimistic lock with {@link LockMode#PESSIMISTIC_WRITE}.
		 * </ul>
		 *
		 * This operation cascades to associated instances if the association is
		 * mapped with {@link org.hibernate.annotations.CascadeType#LOCK}.
		 * <p>
		 * Note that the optimistic lock modes {@link LockMode#OPTIMISTIC} and
		 * {@link LockMode#OPTIMISTIC_FORCE_INCREMENT} are not currently supported.
		 *
		 * @param entity a persistent or transient instance
		 * @param lockMode the lock level
		 */
		CompletionStage<Session> lock(Object entity, LockMode lockMode);

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
		 * @see javax.persistence.EntityManager#flush()
		 */
		CompletionStage<Session> flush();

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

		/**
		 * Create an instance of {@link Query} for the given HQL/JPQL query
		 * string.
		 *
		 * @param queryString The HQL/JPQL query
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createQuery(String)
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
		 * @see javax.persistence.EntityManager#createQuery(String, Class)
		 */
		<R> Query<R> createQuery(String queryString, Class<R> resultType);

		/**
		 * Create an instance of {@link Query} for the named query.
		 *
		 * @param queryName The name of the query
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createQuery(String)
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
		 * @see javax.persistence.EntityManager#createQuery(String, Class)
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
		 * must have multiple columns, which will be returned in arrays.</li>
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
		 * @see javax.persistence.EntityManager#createNativeQuery(String, Class)
		 */
		<R> Query<R> createNativeQuery(String queryString, Class<R> resultType);

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
		 * @see javax.persistence.EntityManager#createNativeQuery(String, String)
		 */
		<R> Query<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping);

		/**
		 * Create an instance of {@link Query} for the given SQL update, insert,
		 * or delete DML statement.
		 *
		 * @param queryString The SQL update, insert, or delete statement
		 */
		Query<Integer> createNativeQuery(String queryString);

		/**
		 * Create an instance of {@link Query} for the given criteria query.
		 *
		 * @param criteriaQuery The {@link CriteriaQuery}
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createQuery(String)
		 */
		<R> Query<R> createQuery(CriteriaQuery<R> criteriaQuery);

		/**
		 * Create an instance of {@link Query} for the given criteria update.
		 *
		 * @param criteriaUpdate The {@link CriteriaUpdate}
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createQuery(String)
		 */
		<R> Query<R> createQuery(CriteriaUpdate<R> criteriaUpdate);

		/**
		 * Create an instance of {@link Query} for the given criteria delete.
		 *
		 * @param criteriaDelete The {@link CriteriaDelete}
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createQuery(String)
		 */
		<R> Query<R> createQuery(CriteriaDelete<R> criteriaDelete);

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
		 * mapped with {@link javax.persistence.CascadeType#DETACH}.
		 *
		 * @param entity The entity to evict
		 *
		 * @throws NullPointerException if the passed object is {@code null}
		 * @throws IllegalArgumentException if the passed object is not defined as an entity
		 *
		 * @see javax.persistence.EntityManager#detach(Object)
		 */
		Session detach(Object entity);

		/**
		 * Completely clear the session. Detach all persistent instances and cancel
		 * all pending insertions, updates and deletions.
		 *
		 * @see javax.persistence.EntityManager#clear()
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
		 * {@link javax.persistence.SqlResultSetMapping}.
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
		 * Get the current cache mode.
		 *
		 * @return The current cache mode.
		 */
		CacheMode getCacheMode();

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
		 *
		 * @param work a function which accepts {@link Transaction} and returns
		 *             the result of the work as a {@link CompletionStage}.
		 */
		<T> CompletionStage<T> withTransaction(Function<Transaction, CompletionStage<T>> work);

		/**
		 * Close the reactive session and release the underlying database
		 * connection.
		 */
		void close();

		/**
		 * @return false if {@link #close()} has been called
		 */
		boolean isOpen();
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
	 * {@link javax.persistence.EntityManagerFactory} as follows:
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
		 * Obtain a new {@link Session reactive session}, the main
		 * interaction point between the user's program and Hibernate
		 * Reactive.
		 * <p>
		 * The underlying database connection is obtained lazily
		 * when the returned {@link Session} needs to access the
		 * database.
		 * <p>
		 * The client must close the session using {@link Session#close()}.
		 */
		Session createSession();

		/**
		 * Obtain a new {@link Session reactive session}, the main
		 * interaction point between the user's program and Hibernate
		 * Reactive.
		 * <p>
		 * The underlying database connection is obtained before the
		 * {@link Session} is returned via a {@link CompletionStage}.
		 * <p>
		 * The client must close the session using {@link Session#close()}.
		 */
		CompletionStage<Session> openSession();

		/**
		 * Perform work using a {@link Session reactive session}.
		 * <p>
		 * The session will be closed automatically.
		 *
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link CompletionStage}.
		 */
		<T> CompletionStage<T> withSession(Function<Session, CompletionStage<T>> work);

		/**
		 * Perform work using a {@link Session reactive session} within an
		 * associated {@link Transaction transaction}.
		 * <p>
		 * The session will be {@link Session#flush() flushed} and closed
		 * automatically, and the transaction committed automatically.
		 *
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link CompletionStage}.
		 *
		 * @see #withSession(Function)
		 * @see Session#withTransaction(Function)
		 */
		<T> CompletionStage<T> withTransaction(BiFunction<Session, Transaction, CompletionStage<T>> work);

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
		 * Destroy the session factory and clean up its connection pool.
		 */
		void close();

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

		SharedSessionContractImplementor session;
		if ( association instanceof HibernateProxy) {
			session = ( (HibernateProxy) association ).getHibernateLazyInitializer().getSession();
		}
		else if ( association instanceof PersistentCollection) {
			session = ( (AbstractPersistentCollection) association ).getSession();
		}
		else {
			return CompletionStages.completedFuture( association );
		}
		if (session==null) {
			throw new LazyInitializationException("session closed");
		}
		return ( (ReactiveSession) session ).reactiveFetch( association, false );
	}
}
