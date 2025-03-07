/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;

import jakarta.persistence.EntityGraph;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**Mutiny
 * A contract with the Hibernate stateless session backing the user-visible
 * {@link org.hibernate.reactive.stage.Stage.StatelessSession reactive session}.
 * <p>
 * This is primarily an internal contract between the various subsystems
 * of Hibernate Reactive.
 *
 * @see org.hibernate.reactive.stage.Stage.Session
 * @see org.hibernate.reactive.mutiny.Mutiny.Session
 */
@Incubating
public interface ReactiveStatelessSession extends ReactiveQueryProducer, ReactiveSharedSessionContractImplementor {

	<T> CompletionStage<T> reactiveGet(Class<T> entityClass, Object id);

	<T> CompletionStage<List<T>> reactiveGet(Class<T> entityClass, Object... id);

	<T> CompletionStage<T> reactiveGet(String entityName, Object id);

	<T> CompletionStage<T> reactiveGet(Class<T> entityClass, Object id, LockMode lockMode, EntityGraph<T> fetchGraph);

	<T> CompletionStage<T> reactiveGet(String entityName, Object id, LockMode lockMode, EntityGraph<T> fetchGraph);

	CompletionStage<Void> reactiveInsert(Object entity);

	CompletionStage<Void> reactiveDelete(Object entity);

	CompletionStage<Void> reactiveUpdate(Object entity);

	CompletionStage<Void> reactiveUpsert(Object entity);

	CompletionStage<Void> reactiveUpsert(String entityName, Object entity);

	CompletionStage<Void> reactiveUpsertAll(int batchSize, Object... entities);

	CompletionStage<Void> reactiveRefresh(Object entity);

	CompletionStage<Void> reactiveRefresh(String entityName, Object entity);

	CompletionStage<Void> reactiveRefresh(Object entity, LockMode lockMode);

	CompletionStage<Void> reactiveRefresh(String entityName, Object entity, LockMode lockMode);

	CompletionStage<Void> reactiveInsertAll(Object... entities);

	CompletionStage<Void> reactiveInsertAll(int batchSize, Object... entities);

	CompletionStage<Void> reactiveUpdateAll(Object... entities);

	CompletionStage<Void> reactiveUpdateAll(int batchSize, Object... entities);

	CompletionStage<Void> reactiveDeleteAll(Object... entities);

	CompletionStage<Void> reactiveDeleteAll(int batchSize, Object... entities);

	CompletionStage<Void> reactiveRefreshAll(Object... entities);

	CompletionStage<Void> reactiveRefreshAll(int batchSize, Object... entities);

	boolean isOpen();

	void close(CompletableFuture<Void> closing);

	Object getIdentifier(Object entity);
}
