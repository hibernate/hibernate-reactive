package org.hibernate.rx;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface RxQuery<R> {

	RxQuery<R> setParameter(int var1, Object var2);

	RxQuery<R> setMaxResults(int var1);

	RxQuery<R> setFirstResult(int var1);

	/**
	 * Like the JPA version, this expects to be able to load
	 * strictly one result.
	 * More than one results will cause an exception.
	 * Zero results will also cause an exception.
	 */
	CompletionStage<R> getSingleResult();

	CompletionStage<List<R>> getResultList();

	 <T> T unwrap(Class<T> type);

	/**

	Some examples of additional useful methods to add here:

	String getQueryString();

	RxQuery<R> setCacheable(boolean var1);

	RxQuery<R> setCacheRegion(String var1);

	RxQuery<R> setCacheMode(CacheMode var1);

	RxQuery<R> setTimeout(int var1);

	RxQuery<R> setFetchSize(int var1);

	RxQuery<R> setReadOnly(boolean var1);

	RxQuery<R> setComment(String var1);

	RxQuery<R> setLockMode(String var1, LockMode var2);

	*/

}
