package org.hibernate.rx;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.Single;

public interface RxQuery<R> {

	RxQuery<R> setParameter(int var1, Object var2);

	RxQuery<R> setMaxResults(int var1);

	RxQuery<R> setFirstResult(int var1);

	Single<R> getSingleResult();

	Maybe<R> getOneResultMaybe();

	Flowable<R> resultsFlow();

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
