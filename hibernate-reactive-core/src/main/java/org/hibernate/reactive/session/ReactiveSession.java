/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import java.util.List;
import java.util.Map;

import org.hibernate.CacheMode;
import org.hibernate.Filter;
import org.hibernate.FlushMode;
import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.UnknownProfileException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.event.spi.DeleteContext;
import org.hibernate.event.spi.MergeContext;
import org.hibernate.event.spi.PersistContext;
import org.hibernate.event.spi.RefreshContext;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.engine.impl.InternalStage;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;

import jakarta.persistence.EntityGraph;
import jakarta.persistence.metamodel.Attribute;

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
public interface ReactiveSession extends ReactiveQueryProducer, ReactiveSharedSessionContractImplementor {

	ReactiveActionQueue getReactiveActionQueue();

	SessionImplementor getSharedContract();

	<E,T> InternalStage<T> reactiveFetch(E entity, Attribute<E,T> field);

	InternalStage<Void> reactivePersist(Object entity);

	InternalStage<Void> reactivePersist(Object object, PersistContext copiedAlready);

	InternalStage<Void> reactivePersistOnFlush(Object entity, PersistContext copiedAlready);

	InternalStage<Void> reactiveRemove(Object entity);

	InternalStage<Void> reactiveRemove(String entityName, boolean isCascadeDeleteEnabled, DeleteContext transientObjects);

	InternalStage<Void> reactiveRemove(String entityName, Object child, boolean isCascadeDeleteEnabled, DeleteContext transientEntities);

	<T> InternalStage<T> reactiveMerge(T object);

	InternalStage<Void> reactiveMerge(Object object, MergeContext copiedAlready);

	InternalStage<Void> reactiveFlush();

	InternalStage<Void> reactiveAutoflush();

	InternalStage<Void> reactiveForceFlush(EntityEntry entry);

	InternalStage<Void> reactiveRefresh(Object entity, LockOptions lockMode);

	InternalStage<Void> reactiveRefresh(Object child, RefreshContext refreshedAlready);

	InternalStage<Void> reactiveLock(Object entity, LockOptions lockMode);

	<T> InternalStage<T> reactiveGet(Class<T> entityClass, Object id);

	<T> InternalStage<T> reactiveFind(Class<T> entityClass, Object id, LockOptions lockOptions, EntityGraph<T> fetchGraph);

	<T> InternalStage<List<T>> reactiveFind(Class<T> entityClass, Object... ids);

	<T> InternalStage<T> reactiveFind(Class<T> entityClass, Map<String,Object> naturalIds);

	InternalStage<Object> reactiveImmediateLoad(String entityName, Object id);

	InternalStage<Void> reactiveInitializeCollection(PersistentCollection<?> collection, boolean writing);

	InternalStage<Void> reactiveRemoveOrphanBeforeUpdates(String entityName, Object child);

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

	String getEntityName(Object entity);
	Object getIdentifier(Object entity);
	boolean contains(Object entity);

	<T> Class<? extends T> getEntityClass(T entity);
	Object getEntityId(Object entity);

	LockMode getCurrentLockMode(Object entity);

	Filter enableFilter(String filterName);
	void disableFilter(String filterName);
	Filter getEnabledFilter(String filterName);

//	SessionStatistics getStatistics();

	boolean isFetchProfileEnabled(String name) throws UnknownProfileException;
	void enableFetchProfile(String name) throws UnknownProfileException;
	void disableFetchProfile(String name) throws UnknownProfileException;

	void clear();

	boolean isDirty();
	boolean isOpen();

	// Different approach so that we can overload the method in SessionImpl
	InternalStage<Void> reactiveClose();
}
