/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import org.hibernate.Incubating;
import org.hibernate.LockMode;

import javax.persistence.EntityGraph;
import java.util.concurrent.CompletionStage;

/**
 * A contract with the Hibernate stateless session backing the user-visible
 * {@link org.hibernate.reactive.stage.Stage.StatelessSession reactive session}.
 * <p>
 * This is primarily an internal contract between the various subsystems
 * of Hibernate Reactive.
 *
 *  @see org.hibernate.reactive.stage.Stage.Session
 *  @see org.hibernate.reactive.mutiny.Mutiny.Session
 */
@Incubating
public interface ReactiveStatelessSession extends ReactiveQueryExecutor {

    <T> CompletionStage<T> reactiveGet(Class<? extends T> entityClass, Object id);

    <T> CompletionStage<T> reactiveGet(Class<? extends T> entityClass, Object id, LockMode lockMode, EntityGraph<T> fetchGraph);

    CompletionStage<Void> reactiveInsert(Object entity);

    CompletionStage<Void> reactiveDelete(Object entity);

    CompletionStage<Void> reactiveUpdate(Object entity);

    CompletionStage<Void> reactiveRefresh(Object entity);

    CompletionStage<Void> reactiveRefresh(Object entity, LockMode lockMode);

    <R> ReactiveQuery<R> createReactiveQuery(String queryString);

    <R> ReactiveQuery<R> createReactiveQuery(String queryString, Class<R> resultType);

    <T> ReactiveNativeQuery<T> createReactiveNativeQuery(String sqlString);

    <T> ReactiveNativeQuery<T> createReactiveNativeQuery(String sqlString, Class<T> resultType);

    <T> ReactiveNativeQuery<T> createReactiveNativeQuery(String sqlString, String resultSetMapping);

    <T> CompletionStage<T> reactiveFetch(T association, boolean unproxy);

    boolean isOpen();

    void close();

}
