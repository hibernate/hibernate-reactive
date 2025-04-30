/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.delegation;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.LockModeType;
import jakarta.persistence.criteria.CriteriaQuery;
import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.mutiny.Mutiny;

import java.util.function.Function;

/**
 * Wraps a {@linkplain #delegate} stateless session.
 *
 * @author Gavin King
 */
public abstract class MutinyStatelessSessionDelegator implements Mutiny.StatelessSession {

    public abstract Mutiny.StatelessSession delegate();

    public <T> Uni<T> get(Class<T> entityClass, Object id) {
        return delegate().get(entityClass, id);
    }

    @Deprecated
    public <R> Mutiny.Query<R> createQuery(String queryString) {
        return delegate().createQuery(queryString);
    }

    public <T> Uni<T> get(EntityGraph<T> entityGraph, Object id) {
        return delegate().get(entityGraph, id);
    }

    public <R> Mutiny.SelectionQuery<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
        return delegate().createNativeQuery(queryString, resultSetMapping);
    }

    public <T> Uni<T> get(Class<T> entityClass, Object id, LockMode lockMode) {
        return delegate().get(entityClass, id, lockMode);
    }

    public boolean isOpen() {
        return delegate().isOpen();
    }

    public Uni<Void> insertAll(Object... entities) {
        return delegate().insertAll(entities);
    }

    public Uni<Void> updateAll(int batchSize, Object... entities) {
        return delegate().updateAll(batchSize, entities);
    }

    public <T> EntityGraph<T> createEntityGraph(Class<T> rootType, String graphName) {
        return delegate().createEntityGraph(rootType, graphName);
    }

    public <T> EntityGraph<T> getEntityGraph(Class<T> rootType, String graphName) {
        return delegate().getEntityGraph(rootType, graphName);
    }

    public <T> Uni<T> get(Class<T> entityClass, Object id, LockModeType lockModeType) {
        return delegate().get(entityClass, id, lockModeType);
    }

    public Uni<Void> update(Object entity) {
        return delegate().update(entity);
    }

    public Uni<Void> refreshAll(int batchSize, Object... entities) {
        return delegate().refreshAll(batchSize, entities);
    }

    public <R> Mutiny.SelectionQuery<R> createQuery(String queryString, Class<R> resultType) {
        return delegate().createQuery(queryString, resultType);
    }

    public Uni<Void> delete(Object entity) {
        return delegate().delete(entity);
    }

    public Uni<Void> refresh(Object entity, LockModeType lockModeType) {
        return delegate().refresh(entity, lockModeType);
    }

    public Mutiny.SessionFactory getFactory() {
        return delegate().getFactory();
    }

    public <R> Mutiny.SelectionQuery<R> createNativeQuery(String queryString, Class<R> resultType) {
        return delegate().createNativeQuery(queryString, resultType);
    }

    public Uni<Void> deleteAll(Object... entities) {
        return delegate().deleteAll(entities);
    }

    public <R> Mutiny.SelectionQuery<R> createSelectionQuery(String queryString, Class<R> resultType) {
        return delegate().createSelectionQuery(queryString, resultType);
    }

    @Incubating
    public Uni<Void> upsert(Object entity) {
        return delegate().upsert(entity);
    }

    public Mutiny.MutationQuery createMutationQuery(String queryString) {
        return delegate().createMutationQuery(queryString);
    }

    public Uni<Void> refresh(Object entity) {
        return delegate().refresh(entity);
    }

    public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
        return delegate().getResultSetMapping(resultType, mappingName);
    }

    public Uni<Void> insertAll(int batchSize, Object... entities) {
        return delegate().insertAll(batchSize, entities);
    }

    public <R> Mutiny.Query<R> createNativeQuery(String queryString) {
        return delegate().createNativeQuery(queryString);
    }

    public <R> Mutiny.Query<R> createNamedQuery(String queryName) {
        return delegate().createNamedQuery(queryName);
    }

    public <R> Mutiny.SelectionQuery<R> createNamedQuery(String queryName, Class<R> resultType) {
        return delegate().createNamedQuery(queryName, resultType);
    }

    public Uni<Void> refreshAll(Object... entities) {
        return delegate().refreshAll(entities);
    }

    public Uni<Void> close() {
        return delegate().close();
    }

    public Uni<Void> updateAll(Object... entities) {
        return delegate().updateAll(entities);
    }

    public <R> Mutiny.SelectionQuery<R> createQuery(CriteriaQuery<R> criteriaQuery) {
        return delegate().createQuery(criteriaQuery);
    }

    public <T> Uni<T> withTransaction(Function<Mutiny.Transaction, Uni<T>> work) {
        return delegate().withTransaction(work);
    }

    public <T> Uni<T> fetch(T association) {
        return delegate().fetch(association);
    }

    @Incubating
    public Uni<Void> upsert(String entityName, Object entity) {
        return delegate().upsert(entityName, entity);
    }

    @Incubating
    public Mutiny.Transaction currentTransaction() {
        return delegate().currentTransaction();
    }

    public <T> EntityGraph<T> createEntityGraph(Class<T> rootType) {
        return delegate().createEntityGraph(rootType);
    }

    public Uni<Void> insert(Object entity) {
        return delegate().insert(entity);
    }

    public Uni<Void> refresh(Object entity, LockMode lockMode) {
        return delegate().refresh(entity, lockMode);
    }

    public Uni<Void> deleteAll(int batchSize, Object... entities) {
        return delegate().deleteAll(batchSize, entities);
    }
}
