/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.delegation;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.LockModeType;
import jakarta.persistence.TypedQueryReference;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import org.hibernate.Incubating;
import org.hibernate.LockMode;
import org.hibernate.query.criteria.JpaCriteriaInsert;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.mutiny.Mutiny;

import java.util.List;
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

    public <T> Uni<List<T>> get(Class<T> entityClass, Object... ids) {
        return delegate().get(entityClass, ids);
    }

    public <R> Mutiny.SelectionQuery<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
        return delegate().createNativeQuery(queryString, resultSetMapping);
    }

    public Object getIdentifier(Object entity) {
        return delegate().getIdentifier(entity);
    }

    public <T> Uni<T> get(Class<T> entityClass, Object id, LockMode lockMode) {
        return delegate().get(entityClass, id, lockMode);
    }

    public boolean isOpen() {
        return delegate().isOpen();
    }

    public CriteriaBuilder getCriteriaBuilder() {
        return delegate().getCriteriaBuilder();
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

    public <R> Mutiny.Query<R> createQuery(TypedQueryReference<R> typedQueryReference) {
        return delegate().createQuery(typedQueryReference);
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

    public Uni<Void> deleteMultiple(List<?> entities) {
        return delegate().deleteMultiple(entities);
    }

    public Uni<Void> deleteAll(Object... entities) {
        return delegate().deleteAll(entities);
    }

    public Uni<Void> updateMultiple(List<?> entities) {
        return delegate().updateMultiple(entities);
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

    @Incubating
    public Uni<Void> upsertMultiple(List<?> entities) {
        return delegate().upsertMultiple(entities);
    }

    public <R> Mutiny.MutationQuery createQuery(CriteriaUpdate<R> criteriaUpdate) {
        return delegate().createQuery(criteriaUpdate);
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

    public <R> Mutiny.MutationQuery createQuery(CriteriaDelete<R> criteriaDelete) {
        return delegate().createQuery(criteriaDelete);
    }

    public <T> Uni<T> withTransaction(Function<Mutiny.Transaction, Uni<T>> work) {
        return delegate().withTransaction(work);
    }

    public Uni<Void> refreshMultiple(List<?> entities) {
        return delegate().refreshMultiple(entities);
    }

    public <T> Uni<T> fetch(T association) {
        return delegate().fetch(association);
    }

    @Incubating
    public Uni<Void> upsertAll(Object... entities) {
        return delegate().upsertAll(entities);
    }

    public Mutiny.Transaction currentTransaction() {
        return delegate().currentTransaction();
    }

    @Incubating
    public Uni<Void> upsertAll(int batchSize, Object... entities) {
        return delegate().upsertAll(batchSize, entities);
    }

    public <T> EntityGraph<T> createEntityGraph(Class<T> rootType) {
        return delegate().createEntityGraph(rootType);
    }

    public Uni<Void> insert(Object entity) {
        return delegate().insert(entity);
    }

    public Uni<Void> insertMultiple(List<?> entities) {
        return delegate().insertMultiple(entities);
    }

    public Uni<Void> refresh(Object entity, LockMode lockMode) {
        return delegate().refresh(entity, lockMode);
    }

    public Uni<Void> deleteAll(int batchSize, Object... entities) {
        return delegate().deleteAll(batchSize, entities);
    }

    @Override
    public Mutiny.MutationQuery createMutationQuery(CriteriaUpdate<?> updateQuery) {
        return delegate().createMutationQuery(updateQuery);
    }

    @Override
    public Mutiny.MutationQuery createMutationQuery(CriteriaDelete<?> deleteQuery) {
        return delegate().createMutationQuery(deleteQuery);
    }

    @Override
    public Mutiny.MutationQuery createMutationQuery(JpaCriteriaInsert<?> insert) {
        return delegate().createMutationQuery(insert);
    }

    @Override
    public <R> Mutiny.Query<R> createNativeQuery(String queryString, AffectedEntities affectedEntities) {
        return delegate().createNativeQuery(queryString, affectedEntities);
    }

    @Override
    public <R> Mutiny.SelectionQuery<R> createNativeQuery(String queryString, Class<R> resultType, AffectedEntities affectedEntities) {
        return delegate().createNativeQuery(queryString, resultType, affectedEntities);
    }

    @Override
    public <R> Mutiny.SelectionQuery<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping, AffectedEntities affectedEntities) {
        return delegate().createNativeQuery(queryString, resultSetMapping, affectedEntities);
    }
}
