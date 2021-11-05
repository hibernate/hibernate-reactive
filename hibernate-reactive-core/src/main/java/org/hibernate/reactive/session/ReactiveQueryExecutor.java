/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import org.hibernate.Incubating;
import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.query.spi.sql.NativeSQLQuerySpecification;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.common.ResultSetMapping;

import javax.persistence.EntityGraph;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Executes queries in a non-blocking fashion. An internal contract
 * between the {@link org.hibernate.query.spi.NativeQueryImplementor}s
 * and stateless and stateful reactive sessions.
 *
 * @see SharedSessionContractImplementor
 */
@Incubating
public interface ReactiveQueryExecutor extends ReactiveConnectionSupplier {

	SessionFactoryImplementor getFactory();

	SharedSessionContractImplementor getSharedContract();

	Dialect getDialect();

	<T> CompletionStage<List<T>> reactiveList(String query, QueryParameters parameters);

	<T> CompletionStage<List<T>> reactiveList(NativeSQLQuerySpecification spec, QueryParameters parameters);

	CompletionStage<Integer> executeReactiveUpdate(String expandedQuery, QueryParameters parameters);

	CompletionStage<Integer> executeReactiveUpdate(NativeSQLQuerySpecification specification, QueryParameters parameters);

	<T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName);

	void addBulkCleanupAction(BulkOperationCleanupAction action);

	CompletionStage<Object> reactiveInternalLoad(String entityName, Serializable id, boolean eager, boolean nullable);

	<R> ReactiveQuery<R> createReactiveQuery(Criteria<R> criteria);

	<T> ReactiveQuery<T> createReactiveCriteriaQuery(String jpaqlString, Class<T> resultClass, CriteriaQueryOptions queryOptions);

	<T> EntityGraph<T> createEntityGraph(Class<T> entity);

	<T> EntityGraph<T> createEntityGraph(Class<T> entity, String name);

	<T> EntityGraph<T> getEntityGraph(Class<T> entity, String name);

	<R> ReactiveQuery<R> createReactiveQuery(String queryString);

	<R> ReactiveQuery<R> createReactiveQuery(String queryString, Class<R> resultType);

	<R> ReactiveQuery<R> createReactiveNamedQuery(String name);

	<R> ReactiveQuery<R> createReactiveNamedQuery(String name, Class<R> resultClass);

	<T> ReactiveQuery<T> createReactiveNativeQuery(String sqlString);

	<T> ReactiveQuery<T> createReactiveNativeQuery(String sqlString, Class<T> resultType);

	<T> ReactiveQuery<T> createReactiveNativeQuery(String sqlString, String resultSetMapping);
}
