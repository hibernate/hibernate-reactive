package org.hibernate.reactive.mutiny;

import io.smallrye.mutiny.Uni;
import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.LazyInitializationException;
import org.hibernate.LockMode;
import org.hibernate.collection.internal.AbstractPersistentCollection;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.proxy.HibernateProxy;
import org.hibernate.reactive.session.ReactiveSession;

import javax.persistence.EntityGraph;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.metamodel.Metamodel;
import java.util.List;
import java.util.function.Function;

/**
 * An API for Hibernate Reactive where non-blocking operations are
 * represented by a Mutiny {@link Uni}.
 *
 * The {@link Query}, {@link Session}, and {@link SessionFactory}
 * interfaces declared here are simply non-blocking counterparts to
 * the similarly-named interfaces in Hibernate ORM.
 */
public interface Mutiny {
	/**
	 * A non-blocking counterpart to the Hibernate
	 * {@link org.hibernate.query.Query} interface, allowing reactive
	 * execution of HQL and JPQL queries.
	 *
	 * The semantics of operations on this interface are identical to the
	 * semantics of the similarly-named operations of {@code Query}, except
	 * that the operations are performed asynchronously, returning a
	 * {@link Uni} without blocking the calling thread.
	 *
	 * @see javax.persistence.Query
	 */
	interface Query<R> {

		Query<R> setParameter(int var1, Object var2);

		Query<R> setMaxResults(int maxResults);

		Query<R> setFirstResult(int firstResult);

		/**
		 * Asynchronously Execute this query, returning a single row
		 * that matches the query, or {@code null} if the query returns
		 * no results, throwing an exception if the query returns more
		 * than one matching result.
		 *
		 * @return the single resulting row or <tt>null</tt>
		 *
		 * @see javax.persistence.Query#getSingleResult()
		 */
		Uni<R> getSingleResult();

		/**
		 * Asynchronously execute this query, return the query results
		 * as a {@link List}. If the query contains multiple results per
		 * row, the results are returned in an instance of <tt>Object[]</tt>.
		 *
		 * @return the resulting rows as a {@link List}
		 *
		 * @see javax.persistence.Query#getResultList()
		 */
		Uni<List<R>> getResultList();

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
		 * Set the comment for this query.
		 *
		 * @param comment The human-readable comment
		 */
		Query<R> setComment(String comment);

		/**
		 * (Re)set the current {@link CacheMode} in effect for this query.
		 */
		Query<R> setCacheMode(CacheMode cacheMode);

		/**
		 * Obtain the {@link CacheMode} in effect for this query.  By default,
		 * the query inherits the {@code CacheMode} of the {@link Session}
		 * from which is originates.
		 *
		 * @see Session#getCacheMode()
		 */
		CacheMode getCacheMode();

		/**
		 * Set the {@link LockMode} to use for specified alias (as defined in
		 * the query's <tt>FROM</tt> clause).
		 *
		 * @see org.hibernate.query.Query#setLockMode(String,LockMode)
		 */
		Query<R> setLockMode(String alias, LockMode lockMode);

		/*

		Some examples of additional useful methods to add here:

		String getQueryString();

		Query<R> setCacheable(boolean var1);

		Query<R> setCacheRegion(String var1);

		*/

	}

	/**
	 * A non-blocking counterpart to the Hibernate {@link org.hibernate.Session}
	 * interface, allowing a reactive style of interaction with the database.
	 *
	 * The semantics of operations on this interface are identical to the
	 * semantics of the similarly-named operations of {@code Session}, except
	 * that the operations are performed asynchronously, returning a
	 * {@link Uni} without blocking the calling thread.
	 *
	 * Entities associated with an {@code Session} do not support transparent
	 * lazy association fetching. Instead, {@link #fetch} should be used to
	 * explicitly request asynchronous fetching of an association, or the
	 * association should be fetched eagerly when the entity is first retrieved.
	 *
	 * {@code Session} does not support JPA entity graphs, but Hibernate fetch
	 * profiles may be used instead.
	 *
	 * @see org.hibernate.Session
	 */
	interface Session extends AutoCloseable {

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
		<T> Uni<T> find(Class<T> entityClass, Object id);

		/**
		 * Asynchronously return the persistent instance of the given entity
		 * class with the given identifier, requesting the given {@link LockMode}.
		 *
		 * @see #find(Class,Object)
		 */
		<T> Uni<T> find(Class<T> entityClass, Object id, LockMode lockMode);

		/**
		 * Asynchronously return the persistent instance of the given entity
		 * class with the given identifier, using the given {@link EntityGraph}
		 * as a fetch plan.
		 *
		 * @see #find(Class,Object)
		 */
		<T> Uni<T> find(Class<T> entityClass, Object id, EntityGraph<T> entityGraph);

		/**
		 * Asynchronously return the persistent instances of the given entity
		 * class with the given identifiers, or null if there is no such
		 * persistent instance.
		 *
		 * @param entityClass The entity type
		 * @param ids the identifiers
		 * @return a list of persistent instances and nulls via a {@code CompletionStage}
		 */
		<T> Uni<List<T>> find(Class<T> entityClass, Object... ids);

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
		Uni<Session> persist(Object entity);

		/**
		 * Persist multiple entities.
		 *
		 * @see #persist(Object)
		 */
		Uni<Session> persist(Object... entities);

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
		Uni<Session> remove(Object entity);

		/**
		 * Remove multiple entities.
		 *
		 * @see #remove(Object)
		 */
		Uni<Session> remove(Object... entities);

		/**
		 * Copy the state of the given object onto the persistent object with the same
		 * identifier. If there is no persistent instance currently associated with
		 * the session, it will be loaded. Return the persistent instance. If the
		 * given instance is unsaved, save a copy of and return it as a newly persistent
		 * instance. The given instance does not become associated with the session.
		 * This operation cascades to associated instances if the association is mapped
		 * with {@code cascade="merge"}
		 *
		 * @param entity a detached instance with state to be copied
		 *
		 * @return an updated persistent instance
		 *
		 * @see javax.persistence.EntityManager#merge(Object)
		 */
		<T> Uni<T> merge(T entity);

		/**
		 * Merge multiple entities.
		 *
		 * @see #merge(Object)
		 */
		<T> Uni<Void> merge(T... entities);

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
		Uni<Session> refresh(Object entity);

		/**
		 * Re-read the state of the given instance from the underlying database,
		 * requesting the given {@link LockMode}.
		 *
		 * @see #refresh(Object)
		 */
		Uni<Session> refresh(Object entity, LockMode lockMode);

		/**
		 * Refresh multiple entities.
		 *
		 * @see #refresh(Object)
		 */
		Uni<Session> refresh(Object... entities);

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
		 * @return the fetched association, via a {@code CompletionStage}
		 *
		 * @see Mutiny#fetch(Object)
		 * @see org.hibernate.Hibernate#initialize(Object)
		 */
		<T> Uni<T> fetch(T association);

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
		 * Create an instance of {@link Mutiny.Query} for the named query.
		 *
		 * @param queryName The name of the query
		 *
		 * @return The {@link Mutiny.Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createQuery(String)
		 */
		<R> Query<R> createNamedQuery(String queryName);

		/**
		 * Create an instance of {@link Mutiny.Query} for the named query.
		 *
		 * @param queryName The name of the query
		 * @param resultType the Java type returned in each row of query results
		 *
		 * @return The {@link Mutiny.Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createQuery(String, Class)
		 */
		<R> Query<R> createNamedQuery(String queryName, Class<R> resultType);

//		/**
//		 * Create an instance of {@link Query} for the given SQL query string.
//		 *
//		 * @param queryString The SQL query
//		 *
//		 * @return The {@link Query} instance for manipulation and execution
//		 *
//		 * @see javax.persistence.EntityManager#createNativeQuery(String)
//		 */
//		<R> Query<R> createNativeQuery(String queryString);

		/**
		 * Create an instance of {@link Mutiny.Query} for the given SQL query string.
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
		 * Create an instance of {@link Mutiny.Query} for the given SQL query string.
		 *
		 * @param queryString The SQL query
		 * @param resultSetMapping the name of the result set mapping
		 *
		 * @return The {@link Mutiny.Query} instance for manipulation and execution
		 *
		 * @see javax.persistence.EntityManager#createNativeQuery(String, String)
		 */
		<R> Query<R> createNativeQuery(String queryString, String resultSetMapping);

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
		Session setFlushMode(FlushMode flushMode);

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
		Session detach(Object entity);

		/**
		 * Completely clear the session. Evict all loaded instances and cancel
		 * all pending insertions, updates and deletions.
		 *
		 * @see javax.persistence.EntityManager#clear()
		 */
		Session clear();

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
		Session enableFetchProfile(String name);

		/**
		 * Obtain a named {@link EntityGraph}
		 */
		<T> EntityGraph<T> getEntityGraph(String graphName);

		/**
		 * Create a new mutable {@link EntityGraph}
		 */
		<T> EntityGraph<T> createEntityGraph(Class<T> rootType);

		/**
		 * Create a new mutable copy of a named {@link EntityGraph}
		 */
		EntityGraph<?> createEntityGraph(String graphName);

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
		Session disableFetchProfile(String name);

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
		 Session setDefaultReadOnly(boolean readOnly);

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
		 * Set the cache mode.
		 * <p>
		 * Cache mode determines the manner in which this session can interact with
		 * the second level cache.
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
	 *
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
	 *
	 * Alternatively, a {@code Mutiny.SessionFactory} may be obtained via
	 * programmatic configuration of Hibernate using:
	 *
	 * <pre>
	 * Configuration configuration = new Configuration();
	 * ...
	 * Mutiny.SessionFactory sessionFactory =
	 * 		configuration.buildSessionFactory(
	 * 			new StandardServiceRegistryBuilder()
	 * 				.applySettings( configuration.getProperties() )
	 * 				.build()
	 * 		)
	 * 		.unwrap(Mutiny.SessionFactory.class);
	 * </pre>
	 *
	 */
	interface SessionFactory extends AutoCloseable {

		/**
		 * Obtain a new {@link Session reactive session}, the
		 * main interaction point between the user's program and
		 * Hibernate Reactive.
		 *
		 * The client must close the session using {@link Session#close()}.
		 */
		Uni<Session> openSession();

		/**
		 * Perform work using a {@link Session reactive session}.
		 *
		 * The session will be closed automatically.
		 *
		 * @param work a function which accepts the session and returns
		 *             the result of the work as a {@link Uni}.
		 */
		<T> Uni<T> withSession(Function<Session, Uni<T>> work);

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
		 * Destroy the session factory and clean up its connection
		 * pool.
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
	 * {@code Mutiny.fetch(author.getBook()).thenAccept(book -> print(book.getTitle()));}
	 * </pre>
	 *
	 * @param association a lazy-loaded association
	 *
	 * @return the fetched association, via a {@code CompletionStage}
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
