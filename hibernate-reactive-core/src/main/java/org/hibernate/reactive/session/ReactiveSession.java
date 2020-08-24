/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.UnknownProfileException;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.event.internal.MergeContext;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.reactive.engine.ReactiveActionQueue;

import javax.persistence.EntityGraph;
import javax.persistence.metamodel.Attribute;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * A contract with the Hibernate session backing the user-visible
 * {@link org.hibernate.reactive.stage.Stage.Session reactive session}.
 * <p>
 * This is primarily an internal contract between the various subsystems
 * of Hibernate Reactive.
 *
 *  @see org.hibernate.reactive.stage.Stage.Session
 *  @see org.hibernate.reactive.mutiny.Mutiny.Session
 */
@Incubating
public interface ReactiveSession extends ReactiveQueryExecutor {

	ReactiveActionQueue getReactiveActionQueue();

	PersistenceContext getPersistenceContext();

	@Override
	SessionImplementor getSharedContract();

	<T> CompletionStage<T> reactiveFetch(T association, boolean unproxy);

	<E,T> CompletionStage<T> reactiveFetch(E entity, Attribute<E,T> field);

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

	CompletionStage<Void> reactiveLock(Object entity, LockMode lockMode);

	<T> ReactiveQuery<T> createReactiveNativeQuery(String sqlString);

	<T> ReactiveNativeQuery<T> createReactiveNativeQuery(String sqlString, String resultSetMapping);

	<T> ReactiveQuery<T> createReactiveNativeQuery(String sqlString, Class<T> resultType);

	<T> ReactiveQuery<T> createReactiveQuery(String queryString);

	<T> ReactiveQuery<T> createReactiveQuery(String queryString, Class<T> resultType);

	<R> ReactiveQuery<R> createReactiveNamedQuery(String name);

	<R> ReactiveQuery<R> createReactiveNamedQuery(String name, Class<R> resultClass);

	<R> ReactiveQuery<R> createReactiveQuery(Criteria<R> criteria);

	<T> ReactiveQuery<T> createReactiveCriteriaQuery(
			String jpaqlString,
			Class<T> resultClass,
			CriteriaQueryOptions queryOptions);

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

	void setHibernateFlushMode(FlushMode flushMode);
	FlushMode getHibernateFlushMode();

	void setCacheMode(CacheMode cacheMode);
	CacheMode getCacheMode();

	Integer getBatchSize();
	void setBatchSize(Integer batchSize);

	<T> T getReference(Class<T> entityClass, Object id);

	void detach(Object entity);

	boolean isDefaultReadOnly();
	void setDefaultReadOnly(boolean readOnly);

	void setReadOnly(Object entityOrProxy, boolean readOnly);
	boolean isReadOnly(Object entityOrProxy);

	String getEntityName(Object object);
	Serializable getIdentifier(Object object);
	boolean contains(Object object);

	LockMode getCurrentLockMode(Object object);

	Filter enableFilter(String filterName);
	void disableFilter(String filterName);
	Filter getEnabledFilter(String filterName);

//	SessionStatistics getStatistics();

	boolean isFetchProfileEnabled(String name) throws UnknownProfileException;
	void enableFetchProfile(String name) throws UnknownProfileException;
	void disableFetchProfile(String name) throws UnknownProfileException;

	<T> EntityGraph<T> createEntityGraph(Class<T> entity);
	<T> EntityGraph<T> createEntityGraph(Class<T> entity, String name);
	<T> EntityGraph<T> getEntityGraph(Class<T> entity, String name);

	void clear();

	boolean isDirty();
	boolean isOpen();
	void close();
}
