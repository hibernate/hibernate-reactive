package org.hibernate.rx;

import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A non-blocking counterpart to the Hibernate {@link org.hibernate.Session}
 * interface, allowing a reactive style of interaction with the database.
 *
 * The semantics of operations on this interface are identical to the
 * semantics of the similarly-named operations of {@code Session}, except
 * that the operations are performed asynchronously, returning a
 * {@link CompletionStage} without blocking the calling thread.
 *
 * Entities associated with an {@code RxSession} do not support transparent
 * lazy association fetching. Instead, {@link #fetch} should be used to
 * explicitly request asynchronous fetching of an association, or the
 * association should be fetched eagerly when the entity is first retrieved.
 *
 * {@code RxSession} does not support JPA entity graphs, but Hibernate fetch
 * profiles may be used instead.
 *
 * {@code RxSession} does not (yet) support pessimistic locking, but
 * optimistic locking via {@code @Version} properties is supported.
 *
 * @see org.hibernate.Session
 */
public interface RxSession {

	/**
	 * Asynchronously return the persistent instance of the given entity
	 * class with the given identifier, or null if there is no such
	 * persistent instance. (If the instance is already associated with
	 * the session, return the associated instance. This method never
	 * returns an uninitialized instance.)
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
	 * using {@link #fetch}.
	 *
	 * You should not use this method to determine if an instance exists
	 * (use {@link #find} instead). Use this only to retrieve an instance
	 * that you assume exists, where non-existence would be an actual error.
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
	 * property if the <code>assigned</code> generator is used.) This operation
	 * cascades to associated instances if the association is mapped with
	 * {@code cascade="save-update"}
	 *
	 * <pre>
	 * {@code session.persist(newBook).thenAccept(v -> session.flush());}
	 * </pre>
	 *
	 * @param entity a transient instance of a persistent class
	 * 
	 * @see javax.persistence.EntityManager#persist(Object)
	 */
	CompletionStage<RxSession> persist(Object entity);

	/**
	 * Asynchronously remove a persistent instance from the datastore. The
	 * argument may be an instance associated with the receiving session or
	 * a transient instance with an identifier associated with existing
	 * persistent state. his operation cascades to associated instances if
	 * the association is mapped with {@code cascade="delete"}
	 *
	 * <pre>
	 * {@code session.delete(book).thenAccept(v -> session.flush());}
	 * </pre>
	 *
	 * @param entity the instance to be removed
	 *
	 * @see javax.persistence.EntityManager#remove(Object)
	 */
	CompletionStage<RxSession> remove(Object entity);

	/**
	 * Copy the state of the given object onto the persistent object with the same
	 * identifier. If there is no persistent instance currently associated with
	 * the session, it will be loaded. Return the persistent instance. If the
	 * given instance is unsaved, save a copy of and return it as a newly persistent
	 * instance. The given instance does not become associated with the session.
	 * This operation cascades to associated instances if the association is mapped
	 * with {@code cascade="merge"}
	 *
	 * @param object a detached instance with state to be copied
	 *
	 * @return an updated persistent instance
	 *
	 * @see javax.persistence.EntityManager#merge(Object)
	 */
	<T> CompletionStage<T> merge(T object);

	/**
	 * Re-read the state of the given instance from the underlying database. It is
	 * inadvisable to use this to implement long-running sessions that span many
	 * business tasks. This method is, however, useful in certain special circumstances.
	 * For example
	 * <ul>
	 * <li>where a database trigger alters the object state upon insert or update
	 * <li>after executing direct SQL (eg. a mass update) in the same session
	 * <li>after inserting a <code>Blob</code> or <code>Clob</code>
	 * </ul>
	 *
	 * @param entity a persistent or detached instance
	 *
	 * @see javax.persistence.EntityManager#refresh(Object)
	 */
	CompletionStage<RxSession> refresh(Object entity);

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
	CompletionStage<RxSession> flush();

	/**
	 * Asynchronously fetch an association that's configured for lazy loading.
	 *
	 * <pre>
	 * {@code session.fetch(author.getBook()).thenAccept(book -> print(book.getTitle()));}
	 * </pre>
	 *
	 * @param association a lazy-loaded association
	 *
	 * @return the fetched association, via a {@code CompletionStage}
	 *
	 * @see org.hibernate.Hibernate#initialize(Object)
	 */
	<T> CompletionStage<T> fetch(T association);

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
	 * Create an instance of {@link RxQuery} for the given HQL/JPQL query
	 * string.
	 *
	 * @param queryString The HQL/JPQL query
	 *
	 * @return The {@link RxQuery} instance for manipulation and execution
	 *
	 * @see javax.persistence.EntityManager#createQuery(String)
	 */
	<R> RxQuery<R> createQuery(String queryString);

	/**
	 * Create an instance of {@link RxQuery} for the given HQL/JPQL query
	 * string.
	 *
	 * @param queryString The HQL/JPQL query
	 * @param resultType the Java type returned in each row of query results
	 *
	 * @return The {@link RxQuery} instance for manipulation and execution
	 *
	 * @see javax.persistence.EntityManager#createQuery(String, Class)
	 */
	<R> RxQuery<R> createQuery(String queryString, Class<R> resultType);

	/**
	 * Set the flush mode for this session.
	 *
	 * The flush mode determines the points at which the session is flushed.
	 * <i>Flushing</i> is the process of synchronizing the underlying persistent
	 * store with persistable state held in memory.
	 *
	 * For a logically "read only" session, it is reasonable to set the session's
	 * flush mode to {@link FlushMode#MANUAL} at the start of the session (in
	 * order to achieve some extra performance).
	 *
	 * @param flushMode the new flush mode
	 */
	RxSession setFlushMode(FlushMode flushMode);

	/**
	 * Get the current flush mode for this session.
	 *
	 * @return the flush mode
	 */
	FlushMode getFlushMode();
	/**
	 * Remove this instance from the session cache. Changes to the instance
	 * will not be synchronized with the database. This operation cascades
	 * to associated instances if the association is mapped with
	 * <code>cascade="evict"</code>.
	 *
	 * @param entity The entity to evict
	 *
	 * @throws NullPointerException if the passed object is {@code null}
	 * @throws IllegalArgumentException if the passed object is not defined as an entity
	 *
	 * @see javax.persistence.EntityManager#detach(Object)
	 */
	RxSession detach(Object entity);

	/**
	 * Completely clear the session. Evict all loaded instances and cancel
	 * all pending insertions, updates and deletions.
	 *
	 * @see javax.persistence.EntityManager#clear()
	 */
	RxSession clear();

	/**
	 * Enable a particular fetch profile on this session.  No-op if requested
	 * profile is already enabled.
	 *
	 * @param name The name of the fetch profile to be enabled.
	 * @throws org.hibernate.UnknownProfileException Indicates that the given name does not
	 * match any known profile names
	 *
	 * @see org.hibernate.engine.profile.FetchProfile for discussion of this feature
	 */
	RxSession enableFetchProfile(String name);

	/**
	 * Disable a particular fetch profile on this session.  No-op if requested
	 * profile is already disabled.
	 *
	 * @param name The name of the fetch profile to be disabled.
	 * @throws org.hibernate.UnknownProfileException Indicates that the given name does not
	 * match any known profile names
	 *
	 * @see org.hibernate.engine.profile.FetchProfile for discussion of this feature
	 */
	RxSession disableFetchProfile(String name);

	/**
	 * Is a particular fetch profile enabled on this session?
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
	 *
	 * Read-only entities are not dirty-checked and snapshots of persistent
	 * state are not maintained. Read-only entities can be modified, but
	 * changes are not persisted.
	 *
	 * @see org.hibernate.Session#setDefaultReadOnly(boolean)
	 */
	 RxSession setDefaultReadOnly(boolean readOnly);

	/**
	 * Set an unmodified persistent object to read-only mode, or a read-only
	 * object to modifiable mode. In read-only mode, no snapshot is maintained,
	 * the instance is never dirty checked, and changes are not persisted.
	 *
	 * @see org.hibernate.Session#setReadOnly(Object, boolean)
	 */
	 RxSession setReadOnly(Object entityOrProxy, boolean readOnly);

	/**
	 * Is the specified entity or proxy read-only?
	 *
	 * @see org.hibernate.Session#isReadOnly(Object)
	 */
	boolean isReadOnly(Object entityOrProxy);

	/**
	 * Set the cache mode.
	 * <p>
	 * Cache mode determines the manner in which this session can interact with
	 * the second level cache.
	 *
	 * @param cacheMode The new cache mode.
	 */
	RxSession setCacheMode(CacheMode cacheMode);

	/**
	 * Get the current cache mode.
	 *
	 * @return The current cache mode.
	 */
	CacheMode getCacheMode();

	/**
	 * Enable the named filter for this current session.
	 *
	 * @param filterName The name of the filter to be enabled.
	 *
	 * @return The Filter instance representing the enabled filter.
	 */
	Filter enableFilter(String filterName);

	/**
	 * Disable the named filter for the current session.
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
	 * Close the reactive session.
	 */
	void close();
}
