package org.hibernate.rx;

import org.hibernate.CacheMode;
import org.hibernate.FlushMode;

import java.util.Optional;
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
	<T> CompletionStage<Optional<T>> find(Class<T> entityClass, Object id);

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
	 * property if the <tt>assigned</tt> generator is used.) This operation
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
	CompletionStage<Void> persist(Object entity);

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
	CompletionStage<Void> remove(Object entity);

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
	CompletionStage<Void> flush();

	/**
	 * Asynchronously fetch an association that configued for lazy loading.
	 * (Currently only works for to-one associations, not for collections.)
	 *
	 * <pre>
	 * {@code session.fetch(author.getBook()).thenAccept(book -> print(book.getTitle()));}
	 * </pre>
	 *
	 * @param association a lazy-loaded association
	 *
	 * @return the fetched association, via a {@code CompletionStage}
	 */
	<T> CompletionStage<T> fetch(T association);

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
	<R> RxQuery<R> createQuery(Class<R> resultType, String queryString);

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
	void setFlushMode(FlushMode flushMode);

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
	 * <tt>cascade="evict"</tt>.
	 *
	 * @param entity The entity to evict
	 *
	 * @throws NullPointerException if the passed object is {@code null}
	 * @throws IllegalArgumentException if the passed object is not defined as an entity
	 *
	 * @see javax.persistence.EntityManager#detach(Object)
	 */
	void detach(Object entity);

	/**
	 * Completely clear the session. Evict all loaded instances and cancel
	 * all pending insertions, updates and deletions.
	 *
	 * @see javax.persistence.EntityManager#clear()
	 */
	void clear();

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
	void enableFetchProfile(String name);

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
	void disableFetchProfile(String name);

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
	 * Set the cache mode.
	 * <p/>
	 * Cache mode determines the manner in which this session can interact with
	 * the second level cache.
	 *
	 * @param cacheMode The new cache mode.
	 */
	void setCacheMode(CacheMode cacheMode);

	/**
	 * Get the current cache mode.
	 *
	 * @return The current cache mode.
	 */
	CacheMode getCacheMode();
}
