package org.hibernate.reactive.session;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.event.internal.MergeContext;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.reactive.engine.spi.ReactiveActionQueue;
import org.hibernate.reactive.pool.ReactiveConnection;

/**
 * A Hibernate {@link org.hibernate.Session} backing the user-visible
 * {@link org.hibernate.reactive.stage.Stage.Session reactive session}.
 * This is primarily an internal contract between the various subsystems
 * of Hibernate Reactive, though it also occurs in the schema of some
 * extension points such as
 * {@link org.hibernate.reactive.id.ReactiveIdentifierGenerator}.
 *
 * Note that even though this interface extends {@code Session},
 * allowing it to be passed around through code in Hibernate core, its
 * non-reactive operations are expected to throw
 * {@link UnsupportedOperationException}.
 *
 *  @see org.hibernate.reactive.stage.Stage.Session
 *  @see org.hibernate.reactive.mutiny.Mutiny.Session
 */
@Incubating
public interface ReactiveSession extends org.hibernate.Session {

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

	CompletionStage<Void> reactiveAutoflush();

	CompletionStage<Void> reactiveRefresh(Object entity, LockMode lockMode);

	CompletionStage<?> reactiveRefresh(Object child, IdentitySet refreshedAlready);

	<T> ReactiveQuery<T> createReactiveQuery(String queryString);

	<T> ReactiveQuery<T> createReactiveQuery(String queryString, Class<T> resultType);

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

	ReactiveConnection getReactiveConnection();
}
