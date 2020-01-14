package org.hibernate.rx;

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
 * Entities associated with an {@code RxSession} do not yet support lazy
 * association fetching. All associations used within a unit of work must
 * be fetched eagerly when an entity is first retrieved.
 *
 * {@see org.hibernate.Session}
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
	 * session.find(Book.class, id).thenAccept(book -> print(book.getTitle()));
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
	 * Asynchronously persist the given transient instance, first assigning
	 * a generated identifier. (Or using the current value of the identifier
	 * property if the <tt>assigned</tt> generator is used.) This operation
	 * cascades to associated instances if the association is mapped with
	 * {@code cascade="save-update"}
	 *
	 * <pre>
	 * session.persist(newBook).thenAccept(v -> session.flush());
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
	 * session.delete(book).thenAccept(v -> session.flush());
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
	 * the session.
	 * <p/>
	 * <i>Flushing</i> is the process of synchronizing the underlying
	 * persistent store with state held in memory.
	 *
	 * <pre>
	 * session.flush().thenAccept(v -> print("done saving changes"));
	 * </pre>
	 *
	 * @see javax.persistence.EntityManager#flush()
	 */
	CompletionStage<Void> flush();

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

	StateControl sessionState();
}
