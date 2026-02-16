/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.mutiny.delegation;

import io.smallrye.mutiny.Uni;
import jakarta.persistence.CacheRetrieveMode;
import jakarta.persistence.CacheStoreMode;
import jakarta.persistence.EntityGraph;
import jakarta.persistence.FlushModeType;
import jakarta.persistence.LockModeType;
import jakarta.persistence.TypedQueryReference;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.metamodel.Attribute;
import org.hibernate.*;
import org.hibernate.query.criteria.JpaCriteriaInsert;
import org.hibernate.reactive.common.AffectedEntities;
import org.hibernate.reactive.common.Identifier;
import org.hibernate.reactive.common.ResultSetMapping;
import org.hibernate.reactive.mutiny.Mutiny;

import java.util.List;
import java.util.function.Function;

/**
 * Wraps a {@linkplain #delegate} session.
 *
 * @author Gavin King
 */
public abstract class MutinySessionDelegator implements Mutiny.Session {

    public abstract Mutiny.Session delegate();

    public <T> Uni<T> find(Class<T> entityClass, Object id) {
        return delegate().find(entityClass, id);
    }

    public <T> Uni<T> find(Class<T> entityClass, Object id, LockModeType lockModeType) {
        return delegate().find(entityClass, id, lockModeType);
    }

    public <T> Uni<T> find(Class<T> entityClass, Object id, LockMode lockMode) {
        return delegate().find(entityClass, id, lockMode);
    }

    public <T> Uni<T> find(EntityGraph<T> entityGraph, Object id) {
        return delegate().find(entityGraph, id);
    }

    public <T> Uni<List<T>> find(Class<T> entityClass, Object... ids) {
        return delegate().find(entityClass, ids);
    }

    public <R> Mutiny.SelectionQuery<R> createNamedQuery(String queryName, Class<R> resultType) {
        return delegate().createNamedQuery(queryName, resultType);
    }

    public <R> Mutiny.SelectionQuery<R> createQuery(String queryString, Class<R> resultType) {
        return delegate().createQuery(queryString, resultType);
    }

    public boolean isReadOnly(Object entityOrProxy) {
        return delegate().isReadOnly(entityOrProxy);
    }

    public <R> Mutiny.SelectionQuery<R> createNativeQuery(String queryString, Class<R> resultType, AffectedEntities affectedEntities) {
        return delegate().createNativeQuery(queryString, resultType, affectedEntities);
    }

    public boolean isDefaultReadOnly() {
        return delegate().isDefaultReadOnly();
    }

    public <T> Uni<T> unproxy(T association) {
        return delegate().unproxy(association);
    }

    public Mutiny.MutationQuery createMutationQuery(String queryString) {
        return delegate().createMutationQuery(queryString);
    }

    public Uni<Void> close() {
        return delegate().close();
    }

    public Mutiny.Session disableFetchProfile(String name) {
        return delegate().disableFetchProfile(name);
    }

    public <T> EntityGraph<T> getEntityGraph(Class<T> rootType, String graphName) {
        return delegate().getEntityGraph(rootType, graphName);
    }

    public <R> Mutiny.SelectionQuery<R> createSelectionQuery(String queryString, Class<R> resultType) {
        return delegate().createSelectionQuery(queryString, resultType);
    }

    public Uni<Void> refresh(Object entity, LockModeType lockModeType) {
        return delegate().refresh(entity, lockModeType);
    }

    public Uni<Void> lock(Object entity, LockModeType lockModeType) {
        return delegate().lock(entity, lockModeType);
    }

    public <R> Mutiny.Query<R> createQuery(TypedQueryReference<R> typedQueryReference) {
        return delegate().createQuery(typedQueryReference);
    }

    public <R> Mutiny.SelectionQuery<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping, AffectedEntities affectedEntities) {
        return delegate().createNativeQuery(queryString, resultSetMapping, affectedEntities);
    }

    public Uni<Void> lock(Object entity, LockMode lockMode) {
        return delegate().lock(entity, lockMode);
    }

    @Incubating
    public <T> Uni<T> find(Class<T> entityClass, Identifier<T> naturalId) {
        return delegate().find(entityClass, naturalId);
    }

    public <T> Uni<T> withTransaction(Function<Mutiny.Transaction, Uni<T>> work) {
        return delegate().withTransaction(work);
    }

    public <R> Mutiny.SelectionQuery<R> createNativeQuery(String queryString, ResultSetMapping<R> resultSetMapping) {
        return delegate().createNativeQuery(queryString, resultSetMapping);
    }

    public <T> EntityGraph<T> createEntityGraph(Class<T> rootType, String graphName) {
        return delegate().createEntityGraph(rootType, graphName);
    }

    public Mutiny.Transaction currentTransaction() {
        return delegate().currentTransaction();
    }

    public Mutiny.Session detach(Object entity) {
        return delegate().detach(entity);
    }

    public Mutiny.Session setCacheStoreMode(CacheStoreMode cacheStoreMode) {
        return delegate().setCacheStoreMode(cacheStoreMode);
    }

    public FlushMode getFlushMode() {
        return delegate().getFlushMode();
    }

    public LockMode getLockMode(Object entity) {
        return delegate().getLockMode(entity);
    }

    public <R> Mutiny.Query<R> createNamedQuery(String queryName) {
        return delegate().createNamedQuery(queryName);
    }

    public CriteriaBuilder getCriteriaBuilder() {
        return delegate().getCriteriaBuilder();
    }

    public Mutiny.SessionFactory getFactory() {
        return delegate().getFactory();
    }

    public <R> Mutiny.SelectionQuery<R> createNativeQuery(String queryString, Class<R> resultType) {
        return delegate().createNativeQuery(queryString, resultType);
    }

    public Mutiny.Session setSubselectFetchingEnabled(boolean enabled) {
        return delegate().setSubselectFetchingEnabled(enabled);
    }

    public Mutiny.Session setFlushMode(FlushMode flushMode) {
        return delegate().setFlushMode(flushMode);
    }

    public Uni<Void> remove(Object entity) {
        return delegate().remove(entity);
    }

    public Mutiny.Session setCacheMode(CacheMode cacheMode) {
        return delegate().setCacheMode(cacheMode);
    }

    public Filter enableFilter(String filterName) {
        return delegate().enableFilter(filterName);
    }

    public Mutiny.MutationQuery createMutationQuery(JpaCriteriaInsert<?> insert) {
        return delegate().createMutationQuery(insert);
    }

    public <R> Mutiny.Query<R> createNativeQuery(String queryString, AffectedEntities affectedEntities) {
        return delegate().createNativeQuery(queryString, affectedEntities);
    }

    public Mutiny.Session setReadOnly(Object entityOrProxy, boolean readOnly) {
        return delegate().setReadOnly(entityOrProxy, readOnly);
    }

    public <T> EntityGraph<T> createEntityGraph(Class<T> rootType) {
        return delegate().createEntityGraph(rootType);
    }

    public Uni<Void> refreshAll(Object... entities) {
        return delegate().refreshAll(entities);
    }

    public Mutiny.MutationQuery createMutationQuery(CriteriaDelete<?> deleteQuery) {
        return delegate().createMutationQuery(deleteQuery);
    }

    public Integer getBatchSize() {
        return delegate().getBatchSize();
    }

    public Uni<Void> refresh(Object entity, LockMode lockMode) {
        return delegate().refresh(entity, lockMode);
    }

    public <T> T getReference(Class<T> entityClass, Object id) {
        return delegate().getReference(entityClass, id);
    }

    public <T> T getReference(T entity) {
        return delegate().getReference(entity);
    }

    public Mutiny.Session setBatchSize(Integer batchSize) {
        return delegate().setBatchSize(batchSize);
    }

    public Uni<Void> refresh(Object entity) {
        return delegate().refresh(entity);
    }

    public CacheMode getCacheMode() {
        return delegate().getCacheMode();
    }

    public Uni<Void> mergeAll(Object... entities) {
        return delegate().mergeAll(entities);
    }

    public Uni<Void> persist(Object object) {
        return delegate().persist(object);
    }

    public boolean contains(Object entity) {
        return delegate().contains(entity);
    }

    public Mutiny.MutationQuery createMutationQuery(CriteriaUpdate<?> updateQuery) {
        return delegate().createMutationQuery(updateQuery);
    }

    public int getFetchBatchSize() {
        return delegate().getFetchBatchSize();
    }

    public Mutiny.Session setDefaultReadOnly(boolean readOnly) {
        return delegate().setDefaultReadOnly(readOnly);
    }

    public Mutiny.Session clear() {
        return delegate().clear();
    }

    public <E, T> Uni<T> fetch(E entity, Attribute<E, T> field) {
        return delegate().fetch(entity, field);
    }

    public <R> Mutiny.SelectionQuery<R> createQuery(CriteriaQuery<R> criteriaQuery) {
        return delegate().createQuery(criteriaQuery);
    }

    public Mutiny.Session setCacheRetrieveMode(CacheRetrieveMode cacheRetrieveMode) {
        return delegate().setCacheRetrieveMode(cacheRetrieveMode);
    }

    public Uni<Void> removeAll(Object... entities) {
        return delegate().removeAll(entities);
    }

    public Filter getEnabledFilter(String filterName) {
        return delegate().getEnabledFilter(filterName);
    }

    public void disableFilter(String filterName) {
        delegate().disableFilter(filterName);
    }

    public <R> Mutiny.MutationQuery createQuery(CriteriaDelete<R> criteriaDelete) {
        return delegate().createQuery(criteriaDelete);
    }

    public Mutiny.Session enableFetchProfile(String name) {
        return delegate().enableFetchProfile(name);
    }

    public <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName) {
        return delegate().getResultSetMapping(resultType, mappingName);
    }

    public Uni<Void> flush() {
        return delegate().flush();
    }

    public Uni<Void> persist(String entityName, Object object) {
        return delegate().persist(entityName, object);
    }

    public <R> Mutiny.Query<R> createNativeQuery(String queryString) {
        return delegate().createNativeQuery(queryString);
    }

    public boolean isFetchProfileEnabled(String name) {
        return delegate().isFetchProfileEnabled(name);
    }

    public <T> Uni<T> merge(T entity) {
        return delegate().merge(entity);
    }

    public boolean isSubselectFetchingEnabled() {
        return delegate().isSubselectFetchingEnabled();
    }

    public <R> Mutiny.MutationQuery createQuery(CriteriaUpdate<R> criteriaUpdate) {
        return delegate().createQuery(criteriaUpdate);
    }

    public Mutiny.Session setFetchBatchSize(int batchSize) {
        return delegate().setFetchBatchSize(batchSize);
    }

    public <T> Uni<T> fetch(T association) {
        return delegate().fetch(association);
    }

    public Uni<Void> persistAll(Object... entities) {
        return delegate().persistAll(entities);
    }

    @Deprecated
    public <R> Mutiny.Query<R> createQuery(String queryString) {
        return delegate().createQuery(queryString);
    }

    public Mutiny.Session setFlushMode(FlushModeType flushModeType) {
        return delegate().setFlushMode(flushModeType);
    }

    @Override
    public boolean isOpen() {
        return delegate().isOpen();
    }
}
