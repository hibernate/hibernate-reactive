/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.hibernate.Cache;
import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.LazyInitializationException;
import org.hibernate.LockMode;
import org.hibernate.collection.internal.AbstractPersistentCollection;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.reactive.common.AutoCloseable;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.session.ReactiveSession;

import javax.persistence.EntityGraph;
import javax.persistence.Parameter;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.Metamodel;
import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An API for Hibernate Reactive where non-blocking operations are
 * represented by a Mutiny {@link Uni}.
 * <p>
 * The {@link Query}, {@link Session}, and {@link SessionFactory}
 * interfaces declared here are simply non-blocking counterparts to
 * the similarly-named interfaces in Hibernate ORM.
 */
public interface Mutiny {
	/**
	 * A non-blocking counterpart to the Hibernate
	 * {@link org.hibernate.query.Query} interface, allowing reactive
	 * execution of HQL and JPQL queries.
	 * <p>
	 * The semantics of operations on this interface are identical to the
	 * semantics of the similarly-named operations of {@code Query}, except
	 * that the operations are performed asynchronously, returning a
	 * {@link Uni} without blocking the calling thread.
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
		 * Asynchronously execute this query, returning a single row
		 * that matches the query, or {@code null} if the query returns
		 * no results, throwing an exception if the query returns more
		 * than one matching row. If the query has multiple results per
		 * row, the results are returned in an instance of {@code Object[]}.
		 *
		 * @return the single resulting row or {@code null}
		 *
		 * @see javax.persistence.Query#getSingleResult()
		 */
		Uni<R> getSingleResult();

		/**
		 * Asynchronously execute this query, returning the query results
		 * as a {@link List}, via a {@link Uni}. If the query has multiple
		 * results per row, the results are returned in an instance of
		 * {@code Object[]}. If the query has multiple results per row,
		 * the results are returned in an instance of {@code Object[]}.
		 *
		 * @return the resulting rows as a {@link List}
		 *
		 * @see javax.persistence.Query#getResultList()
		 */
		Uni<List<R>> getResultList();

		/**
		 * Asynchronously execute this query, returning the query results
		 * as a {@link Multi}. If the query has multiple results per row,
		 * the results are returned in an instance of {@code Object[]}.
		 * <p>
		 * For now, this operation does no more than simply repackage the
		 * result of {@link #getResultList()} as a {@link Multi} for
		 * convenience.
		 *
		 * @return the resulting rows via a {@link Multi}
		 */
		default Multi<R> getResults() {
			return getResultList().onItem().transformToMulti( list -> Multi.createFrom().iterable( list ) );
		}

		/**
		 * Asynchronously execute this delete, update, or insert query,
		 * returning the updated row count.
		 *
		 * @return the row count as an integer
		 *
		 * @see javax.persistence.Query#executeUpdate()
		 */
		Uni<Integer> executeUpdate();

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
	 * that the operations are performed asynchronously, returning a {@link Uni}
	 * without blocking the calling thread.
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
		 * {@code session.find(Book.class, id).map(book -> print(book.getTitle()));}
		 * </pre>
		 *
		 * @param entityClass The entity type
		 * @param id an identifier
		 *
		 * @return a persistent instance or null via a {@code Uni}
		 *
		 * @see javax.persistence.EntityManager#find(Class, Object)
		 */
		<T> Uni<T> find(Class<T> entityClass, Object id);

		/**
		 * Asynchronously return the persistent instance of the given entity
		 * class with the given identifier, requesting the given {@link LockMode}.
		 *
		 * @param entityClass The entity type
		 * @param id an identifier
		 * @param lockMode the requested {@link LockMode}
		 *
		 * @return a persistent instance or null via a {@code Uni}
		 *
		 * @see #find(Class,Object)
		 * @see #lock(Object, LockMode) this discussion of lock modes
		 */
		<T> Uni<T> find(Class<T> entityClass, Object id, LockMode lockMode);

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
		<T> Uni<T> find(EntityGraph<T> entityGraph, Object id);

		/**
		 * Asynchronously return the persistent instances of the given entity
		 * class with the given identifiers, or null if there is no such
		 * persistent instance.
		 *
		 * @param entityClass The entity type
		 * @param ids the identifiers
		 *
		 * @return a list of persistent instances and nulls via a {@code Uni}
		 */
		<T> Uni<List<T>> find(Class<T> entityClass, Object... ids);

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
		 * {@code session.persist(newBook).map(v -> session.flush());}
		 * </pre>
		 *
		 * @param entity a transient instance of a persistent class
		 *
		 * @see javax.persistence.EntityManager#persist(Object)
		 */
		Uni<Session> persist(Object entity);

		/**
		 * Persist multiple transient entity instances at once.
		 *
		 * @see #persist(Object)
		 */
		Uni<Session> persist(Object... entities);

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
		 * @param entity the managed persistent instance to be removed
		 *
		 * @throws IllegalArgumentException if the given instance is not managed
		 *
		 * @see javax.persistence.EntityManager#remove(Object)
		 */
		Uni<Session> remove(Object entity);

		/**
		 * Remove multiple entity instances at once.
		 *
		 * @see #remove(Object)
		 */
		Uni<Session> remove(Object... entities);

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
		<T> Uni<T> merge(T entity);

		/**
		 * Merge multiple entity instances at once.
		 *
		 * @see #merge(Object)
		 */
		<T> Uni<Void> merge(T... entities);

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
		 * @see javax.persistence.EntityManager#refresh(Object)
		 */
		Uni<Session> refresh(Object entity);

		/**
		 * Re-read the state of the given instance from the underlying database,
		 * requesting the given {@link LockMode}.
		 *
		 * @see #refresh(Object)
		 */
		Uni<Session> refresh(Object entity, LockMode lockMode);

		/**
		 * Refresh multiple entity instances at once.
		 *
		 * @see #refresh(Object)
		 */
		Uni<Session> refresh(Object... entities);

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
		Uni<Session> lock(Object entity, LockMode lockMode);

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
		Uni<Session> flush();

		/**
		 * Asynchronously fetch an association that's configured for lazy loading.
		 *
		 * <pre>
		 * {@code session.fetch(author.getBook()).thenAccept(book -> print(book.getTitle()));}
		 * </pre>
		 *
		 * @param association a lazy-loaded association
		 *
		 * @return the fetched association, via a {@code Uni}
		 *
		 * @see Mutiny#fetch(Object)
		 * @see org.hibernate.Hibernate#initialize(Object)
		 */
		<T> Uni<T> fetch(T association);

		/**
		 * Fetch a lazy property of the given entity, identified by a JPA
		 * {@link Attribute attribute metamodel}. Note that this feature is
		 * only supported in conjunction with the Hibernate bytecode enhancer.
		 *
		 * <pre>
		 * {@code session.fetch(book, Book_.isbn).thenAccept(isbn -> print(isbn))}
		 * </pre>
		 */
		<E,T> Uni<T> fetch(E entity, Attribute<E,T> field);

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
		 * @return the fetched association, via a {@code Uni}
		 *
		 * @see org.hibernate.Hibernate#unproxy(Object)
		 */
		<T> Uni<T> unproxy(T association);

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
		 * string or HQL/JPQL update or delete statement. In the case of an
		 * update or delete, the returned {@link Query} must be executed using
		 * {@link Query#executeUpdate()} which returns an affected row count.
		 *
		 * @param queryString The HQL/JPQL query, update or delete statement
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
		 * @return The {@link Mutiny.Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createQuery(String, Class)
		 */
		<R> Query<R> createNamedQuery(String queryName, Class<R> resultType);

		/**
		 * Create an instance of {@link Query} for the given SQL query
		 * string, using the given {@code resultType} to interpret the results.
		 *
		 * <ul>
		 * <li>If the given result type is {@link Object}, or a built-in type
		 * such as {@link String} or {@link Integer}, the result set must
		 * have a single column, which will be returned as a scalar.
		 * <li>If the given result type is {@code Object[]}, then the result set
		 * must have multiple columns, which will be returned in arrays.
		 * <li>Otherwise, the given result type must be an entity class, in which
		 * case the result set column aliases must map to the fields of the
		 * entity, and the query will return instances of the entity.
		 * </ul>
		 *
		 * @param queryString The SQL query
		 * @param resultType the Java type returned in each row of query results
		 *
		 * @return The {@link Mutiny.Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createNativeQuery(String, Class)
		 */
		<R> Query<R> createNativeQuery(String queryString, Class<R> resultType);

		/**
		 * Create an instance of {@link Mutiny.Query} for the given SQL query string,
		 * using the given {@link ResultSetMapping} to interpret the result set.
		 *
		 * @param queryString The SQL query
		 * @param resultSetMapping the result set mapping
		 *
		 * @return The {@link Mutiny.Query} instance for manipulation and execution
		 *
		 * @see #getResultSetMapping(Class, String)
		 * @see javax.persistence.EntityManager#createNativeQuery(String, String)
		 */
		<R> Query<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping);

		/**
		 * Create an instance of {@link Query} for the given  SQL query string,
		 * or SQL update, insert, or delete statement. In the case of an update,
		 * insert or delete, the returned {@link Query} must be executed using
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
		 * Create an instance of {@link Mutiny.Query} for the given criteria query.
		 *
		 * @param criteriaQuery The {@link CriteriaQuery}
		 *
		 * @return The {@link Mutiny.Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createQuery(String)
		 */
		<R> Query<R> createQuery(CriteriaQuery<R> criteriaQuery);

		/**
		 * Create an instance of {@link Mutiny.Query} for the given criteria update.
		 *
		 * @param criteriaUpdate The {@link CriteriaUpdate}
		 *
		 * @return The {@link Mutiny.Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createQuery(String)
		 */
		<R> Query<R> createQuery(CriteriaUpdate<R> criteriaUpdate);

		/**
		 * Create an instance of {@link Mutiny.Query} for the given criteria delete.
		 *
		 * @param criteriaDelete The {@link CriteriaDelete}
		 *
		 * @return The {@link Mutiny.Query} instance for manipulation and execution
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
		 * Set the session-level JDBC batch size, overriding the batch size set
		 * by the configuration property {@code hibernate.jdbc.batch_size}.
		 */
		Session setBatchSize(Integer batchSize);
		/**
		 * The session-level JDBC batch size, or {@code null} if it has not been
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
		 *
		 * @param work a function which accepts {@link Transaction} and returns
		 *             the result of the work as a {@link Uni}.
		 */
		<T> Uni<T> withTransaction(Function<Transaction, Uni<T>> work);

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
	 * A non-blocking counterpart to the Hibernate
	 * {@link org.hibernate.StatelessSession} interface, which provides a
	 * command-oriented API for performing bulk operations against a database.
	 * <p>
	 * A stateless session does not implement a first-level cache nor interact
	 * with any second-level cache, nor does it implement transactional
	 * write-behind or automatic dirty checking, nor do operations cascade to
	 * associated instances. Collections are ignored by a stateless session.
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
	interface StatelessSession extends AutoCloseable {

		/**
		 * Retrieve a row.
		 *
		 * @param entityClass The class of the entity to retrieve
		 * @param id The id of the entity to retrieve
		 *
		 * @return a detached entity instance, via a {@code Uni}
		 *
		 * @see org.hibernate.StatelessSession#get(Class, Serializable)
		 */
		<T> Uni<T> get(Class<T> entityClass, Object id);

		/**
		 * Retrieve a row, obtaining the specified lock mode.
		 *
		 * @param entityClass The class of the entity to retrieve
		 * @param id The id of the entity to retrieve
		 * @param lockMode The lock mode to apply to the entity
		 *
		 * @return a detached entity instance, via a {@code Uni}
		 *
		 * @see org.hibernate.StatelessSession#get(Class, Serializable, LockMode)
		 */
		<T> Uni<T> get(Class<T> entityClass, Object id, LockMode lockMode);

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
		 * @see Session#createQuery(String)
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
		 * @see Session#createQuery(String, Class)
		 */
		<R> Query<R> createQuery(String queryString, Class<R> resultType);

		/**
		 * Create an instance of {@link Query} for the given  SQL query string,
		 * or SQL update, insert, or delete statement. In the case of an update,
		 * insert or delete, the returned {@link Query} must be executed using
		 * {@link Query#executeUpdate()} which returns an affected row count.
		 * In the case of a query:
		 *
		 * @param queryString The SQL select, update, insert, or delete statement
		 *
		 * @see Session#createNativeQuery(String)
		 */
		<R> Query<R> createNativeQuery(String queryString);

		/**
		 * Create an instance of {@link Query} for the given SQL query string,
		 * using the given {@code resultType} to interpret the results.
		 *
		 * @param queryString The SQL query
		 * @param resultType the Java type returned in each row of query results
		 *
		 * @return The {@link Query} instance for manipulation and execution
		 *
		 * @see Session#createNativeQuery(String, Class)
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
		 * @see Session#createNativeQuery(String, ResultSetMapping)
		 */
		<R> Query<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping);

		/**
		 * Insert a row.
		 *
		 * @param entity a new transient instance
		 *
		 * @see org.hibernate.StatelessSession#insert(Object)
		 */
		Uni<StatelessSession> insert(Object entity);

		/**
		 * Delete a row.
		 *
		 * @param entity a detached entity instance
		 *
		 * @see org.hibernate.StatelessSession#delete(Object)
		 */
		Uni<StatelessSession> delete(Object entity);

		/**
		 * Update a row.
		 *
		 * @param entity a detached entity instance
		 *
		 * @see org.hibernate.StatelessSession#update(Object)
		 */
		Uni<StatelessSession> update(Object entity);

		/**
		 * Refresh the entity instance state from the database.
		 *
		 * @param entity The entity to be refreshed.
		 *
		 * @see org.hibernate.StatelessSession#refresh(Object)
		 */
		Uni<StatelessSession> refresh(Object entity);

		/**
		 * Refresh the entity instance state from the database.
		 *
		 * @param entity The entity to be refreshed.
		 * @param lockMode The LockMode to be applied.
		 *
		 * @see org.hibernate.StatelessSession#refresh(Object, LockMode)
		 */
		Uni<StatelessSession> refresh(Object entity, LockMode lockMode);

		/**
		 * Obtain a native SQL result set mapping defined via the annotation
		 * {@link javax.persistence.SqlResultSetMapping}.
		 */
		<T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName);

		/**
		 * @return false if {@link #close()} has been called
		 */
		@Override
		boolean isOpen();

		/**
		 * Close the reactive session and release the underlying database
		 * connection.
		 */
		@Override
		void close();
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
	 * A {@code Mutiny.SessionFactory} may be obtained from an instance of
	 * {@link javax.persistence.EntityManagerFactory} as follows:
	 *
	 * <pre>
	 * Mutiny.SessionFactory sessionFactory =
	 * 			createEntityManagerFactory("example")
	 * 				.unwrap(Mutiny.SessionFactory.class);
	 * </pre>
	 *
	 * Here, configuration properties must be specified in
	 * {@code persistence.xml}.
	 * <p>
	 * Alternatively, a {@code Mutiny.SessionFactory} may be obtained via
	 * programmatic configuration of Hibernate using:
	 *
	 * <pre>
	 * Configuration configuration = new Configuration();
	 * ...
	 * Mutiny.SessionFactory sessionFactory =
	 * 		configuration.buildSessionFactory(
	 * 			new ReactiveServiceRegistryBuilder()
	 * 				.applySettings( configuration.getProperties() )
	 * 				.build()
	 * 		)
	 * 		.unwrap(Mutiny.SessionFactory.class);
	 * </pre>
	 *
	 */
	interface SessionFactory extends AutoCloseable {

		/**
		 * Obtain a new {@link Session reactive session}, the main
		 * interaction point between the user's program and Hibernate
		 * Reactive.
		 * <p>
		 * The underlying database connection is obtained lazily when
		 * the returned {@link Session} needs to access the database.
		 * <p>
		 * The client must explicitly close the session by calling
		 * {@link Session#close()}.
		 *
		 * @see #withSession(Function)
		 */
		Session openSession();

		/**
		 * Obtain a new {@link Session reactive session} for a
		 * specified tenant.
		 * <p>
		 * The underlying database connection is obtained lazily when
		 * the returned {@link Session} needs to access the database.
		 * <p>
		 * The client must explicitly close the session by calling
		 * {@link Session#close()}.
		 *
		 * @param tenantId the id of the tenant
		 *
		 * @see #withSession(Function)
		 */
		Session openSession(String tenantId);

		/**
		 * Obtain a {@link StatelessSession reactive stateless session}.
		 * <p>
		 * The underlying database connection is obtained lazily when
		 * the returned {@link StatelessSession} needs to access the
		 * database.
		 * <p>
		 * The client must explicitly close the session by calling
		 * {@link StatelessSession#close()}.
		 */
		StatelessSession openStatelessSession();

		/**
		 * Perform work using a {@link Session reactive session}.
		 * <p>
		 * The session will be closed automatically.
		 *
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link Uni}.
		 */
		<T> Uni<T> withSession(Function<Session, Uni<T>> work);

		/**
		 * Perform work using a {@link Session reactive session} for
		 * a specified tenant.
		 * <p>
		 * The session will be closed automatically.
		 *
		 * @param tenantId the id of the tenant
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link Uni}.
		 */
		<T> Uni<T> withSession(String tenantId, Function<Session, Uni<T>> work);

		/**
		 * Perform work using a {@link Session reactive session} within an
		 * associated {@link Transaction transaction}.
		 * <p>
		 * The session will be {@link Session#flush() flushed} and closed
		 * automatically, and the transaction committed automatically.
		 *
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link Uni}.
		 *
		 * @see #withSession(Function)
		 * @see Session#withTransaction(Function)
		 */
		<T> Uni<T> withTransaction(BiFunction<Session, Transaction, Uni<T>> work);

		/**
		 * Perform work using a {@link Session reactive session} for a
		 * specified tenant within an associated {@link Transaction transaction}.
		 * <p>
		 * The session will be {@link Session#flush() flushed} and closed
		 * automatically, and the transaction committed automatically.
		 *
		 * @param tenantId the id of the tenant
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link Uni}.
		 *
		 * @see #withSession(Function)
		 * @see Session#withTransaction(Function)
		 */
		<T> Uni<T> withTransaction(String tenantId, BiFunction<Session, Transaction, Uni<T>> work);

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
	 * {@code Mutiny.fetch(author.getBook()).map(book -> print(book.getTitle()));}
	 * </pre>
	 *
	 * @param association a lazy-loaded association
	 *
	 * @return the fetched association, via a {@code Uni}
	 *
	 * @see org.hibernate.Hibernate#initialize(Object)
	 */
	static <T> Uni<T> fetch(T association) {
		if ( association == null ) {
			return Uni.createFrom().nullItem();
		}

		SharedSessionContractImplementor session;
		if ( association instanceof HibernateProxy) {
			session = ( (HibernateProxy) association ).getHibernateLazyInitializer().getSession();
		}
		else if ( association instanceof PersistentCollection) {
			session = ( (AbstractPersistentCollection) association ).getSession();
		}
		else {
			return Uni.createFrom().item( association );
		}
		if (session==null) {
			throw new LazyInitializationException("session closed");
		}
		return Uni.createFrom().completionStage(
				( (ReactiveSession) session ).reactiveFetch( association, false )
		);
	}
}
