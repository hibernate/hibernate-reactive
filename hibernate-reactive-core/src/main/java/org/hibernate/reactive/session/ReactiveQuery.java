package org.hibernate.reactive.session;

import org.hibernate.CacheMode;
import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * An internal contract between the reactive session implementation
 * and the {@link org.hibernate.reactive.stage.Stage.Query} and
 * {@link org.hibernate.reactive.mutiny.Mutiny.Query} APIs.
 *
 * @see ReactiveSession
 */
@Incubating
public interface ReactiveQuery<R> {

	CompletionStage<R> getReactiveSingleResult();

	CompletionStage<List<R>> getReactiveResultList();

	CompletionStage<Integer> executeReactiveUpdate();

	ReactiveQuery<R> setParameter(int position, Object value);

	ReactiveQuery<R> setMaxResults(int maxResults);

	ReactiveQuery<R> setFirstResult(int firstResult);

	ReactiveQuery<R> setReadOnly(boolean readOnly);

	ReactiveQuery<R> setComment(String comment);

	ReactiveQuery<R> setLockMode(String alias, LockMode lockMode);

	ReactiveQuery<R> setCacheMode(CacheMode cacheMode);

	CacheMode getCacheMode();

	ReactiveQuery<R> setResultTransformer(ResultTransformer resultTransformer);

	Type[] getReturnTypes();
}
