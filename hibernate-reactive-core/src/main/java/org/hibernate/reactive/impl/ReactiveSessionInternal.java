package org.hibernate.reactive.impl;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.event.internal.MergeContext;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.reactive.engine.spi.ReactiveActionQueue;
import org.hibernate.reactive.stage.Stage;

/**
 * A Hibernate {@link org.hibernate.Session} backing the user-visible
 * {@link Stage.Session reactive session}. Note that even
 * though this interface extends {@code Session},
 * allowing it to be passed around through code in
 * Hibernate core, its non-reactive operations are
 * expected to throw {@link UnsupportedOperationException}.
 *
 *  @see Stage.Session the actual user visible API
 */
public interface ReactiveSessionInternal extends org.hibernate.Session {

	/**
	 * @return a reactive session backed by this object
	 */
	Stage.Session reactive();

	ReactiveActionQueue getReactiveActionQueue();

	<T> CompletionStage<T> reactiveFetch(T association, boolean unproxy);

	CompletionStage<Void> reactivePersist(Object entity);

	CompletionStage<Void> reactivePersist(Object object, IdentitySet copiedAlready);

	CompletionStage<Void> reactivePersistOnFlush(Object entity, IdentitySet copiedAlready);

	CompletionStage<Void> reactiveRemove(Object entity);

	CompletionStage<Void> reactiveRemove(Object entity, boolean isCascadeDeleteEnabled, IdentitySet transientObjects);

	<T> CompletionStage<T> reactiveMerge(T object);

	CompletionStage<Void> reactiveMerge(Object object, MergeContext copiedAlready);

	CompletionStage<Void> reactiveFlush();

	CompletionStage<Void> reactiveRefresh(Object entity, LockMode lockMode);

	CompletionStage<?> reactiveRefresh(Object child, IdentitySet refreshedAlready);

	<T> ReactiveQueryInternal<T> createReactiveQuery(String queryString);

	<T> ReactiveQueryInternal<T> createReactiveQuery(String queryString, Class<T> resultType);

	<T> CompletionStage<T> reactiveGet(
			Class<T> entityClass,
			Serializable id);

	<T> CompletionStage<T> reactiveFind(
			Class<T> entityClass,
			Object primaryKey,
			LockMode lockMode,
			Map<String, Object> properties);

	<T> CompletionStage<List<T>> reactiveFind(
			Class<T> entityClass,
			Object... primaryKey);

	<T> CompletionStage<List<Object>> reactiveList(String query, QueryParameters queryParameters);

	CompletionStage<Integer> executeReactiveUpdate(String expandedQuery, QueryParameters queryParameters);
}
