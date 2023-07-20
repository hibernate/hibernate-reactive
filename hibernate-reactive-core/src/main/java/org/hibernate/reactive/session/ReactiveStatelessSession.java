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
import org.hibernate.reactive.engine.impl.InternalStage;

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

	<T> InternalStage<T> reactiveGet(Class<? extends T> entityClass, Object id);

	<T> InternalStage<T> reactiveGet(String entityName, Object id);

	<T> InternalStage<T> reactiveGet(Class<? extends T> entityClass, Object id, LockMode lockMode, EntityGraph<T> fetchGraph);

	<T> InternalStage<T> reactiveGet(String entityName, Object id, LockMode lockMode, EntityGraph<T> fetchGraph);

	InternalStage<Void> reactiveInsert(Object entity);

	InternalStage<Void> reactiveDelete(Object entity);

	InternalStage<Void> reactiveUpdate(Object entity);

	InternalStage<Void> reactiveRefresh(Object entity);

	InternalStage<Void> reactiveRefresh(Object entity, LockMode lockMode);

	InternalStage<Void> reactiveInsertAll(Object... entities);

	InternalStage<Void> reactiveInsertAll(int batchSize, Object... entities);

	InternalStage<Void> reactiveUpdateAll(Object... entities);

	InternalStage<Void> reactiveUpdateAll(int batchSize, Object... entities);

	InternalStage<Void> reactiveDeleteAll(Object... entities);

	InternalStage<Void> reactiveDeleteAll(int batchSize, Object... entities);

	InternalStage<Void> reactiveRefreshAll(Object... entities);

	InternalStage<Void> reactiveRefreshAll(int batchSize, Object... entities);

	boolean isOpen();

	void close(InternalStage<Void> closing);
}
