package org.hibernate.rx;

import org.hibernate.CacheMode;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * A non-blocking counterpart to the Hibernate
 * {@link org.hibernate.query.Query} interface, allowing reactive
 * execution of HQL and JPQL queries.
 *
 * The semantics of operations on this interface are identical to the
 * semantics of the similarly-named operations of {@code Query}, except
 * that the operations are performed asynchronously, returning a
 * {@link CompletionStage} without blocking the calling thread.
 *
 * @see javax.persistence.Query
 */
public interface RxQuery<R> {

	RxQuery<R> setParameter(int var1, Object var2);

	RxQuery<R> setMaxResults(int maxResults);

	RxQuery<R> setFirstResult(int firstResult);

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
	CompletionStage<R> getSingleResult();

	/**
	 * Asynchronously execute this query, return the query results
	 * as a {@link List}. If the query contains multiple results per
	 * row, the results are returned in an instance of <tt>Object[]</tt>.
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
	 * @see RxSession#setDefaultReadOnly(boolean)
	 */
	RxQuery<R> setReadOnly(boolean readOnly);

	/**
	 * Set the comment for this query.
	 *
	 * @param comment The human-readable comment
	 */
	RxQuery<R> setComment(String comment);

	/**
	 * (Re)set the current {@link CacheMode} in effect for this query.
	 */
	RxQuery<R> setCacheMode(CacheMode cacheMode);

	/**
	 * Obtain the {@link CacheMode} in effect for this query.  By default,
	 * the query inherits the {@code CacheMode} of the {@link RxSession}
	 * from which is originates.
	 *
	 * @see RxSession#getCacheMode()
	 */
	CacheMode getCacheMode();

	/**

	Some examples of additional useful methods to add here:

	String getQueryString();

	RxQuery<R> setCacheable(boolean var1);

	RxQuery<R> setCacheRegion(String var1);

	RxQuery<R> setTimeout(int var1);

	RxQuery<R> setFetchSize(int var1);

	RxQuery<R> setLockMode(String var1, LockMode var2);

	*/

}
