/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import java.util.concurrent.CompletionStage;

import org.hibernate.Incubating;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.query.criteria.JpaCriteriaInsertSelect;
import org.hibernate.reactive.query.ReactiveMutationQuery;
import org.hibernate.reactive.query.ReactiveNativeQuery;
import org.hibernate.reactive.query.ReactiveQuery;
import org.hibernate.reactive.query.ReactiveSelectionQuery;

import jakarta.persistence.EntityGraph;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.CriteriaUpdate;


/**
 * Executes queries in a non-blocking fashion. An internal contract
 * between the {@link org.hibernate.query.sql.spi.NativeQueryImplementor}s
 * and stateless and stateful reactive sessions.
 *
 * @see org.hibernate.query.QueryProducer
 * @see SharedSessionContractImplementor
 */
@Incubating
public interface ReactiveQueryProducer extends ReactiveConnectionSupplier {

	SessionFactoryImplementor getFactory();

	SharedSessionContractImplementor getSharedContract();

	Dialect getDialect();

	<T> CompletionStage<T> reactiveFetch(T association, boolean unproxy);

	CompletionStage<Object> reactiveInternalLoad(String entityName, Object id, boolean eager, boolean nullable);

	<T> EntityGraph<T> createEntityGraph(Class<T> entity);

	<T> EntityGraph<T> createEntityGraph(Class<T> entity, String name);

	<T> EntityGraph<T> getEntityGraph(Class<T> entity, String name);

	<R> ReactiveQuery<R> createReactiveQuery(String queryString);

	<R> ReactiveQuery<R> createReactiveQuery(String queryString, Class<R> resultType);

	<R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString);

	<R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString, Class<R> resultClass);

	<R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString, Class<R> resultClass, String tableAlias);

	<R> ReactiveNativeQuery<R> createReactiveNativeQuery(String sqlString, String resultSetMappingName, Class<R> resultClass);

	<R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(String hqlString);

	<R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(String hqlString, Class<R> resultType);

	<R> ReactiveSelectionQuery<R> createReactiveSelectionQuery(CriteriaQuery<R> criteria);

	<R> ReactiveMutationQuery<R> createReactiveMutationQuery(String hqlString);

	<R> ReactiveMutationQuery<R> createReactiveMutationQuery(CriteriaUpdate updateQuery);

	<R> ReactiveMutationQuery<R> createReactiveMutationQuery(CriteriaDelete deleteQuery);

	<R> ReactiveMutationQuery<R> createReactiveMutationQuery(JpaCriteriaInsertSelect insertSelect);

	<R> ReactiveMutationQuery<R> createNativeReactiveMutationQuery(String sqlString);

	<R> ReactiveMutationQuery<R> createNamedReactiveMutationQuery(String name, Class<R> resultClass);

	<R> ReactiveSelectionQuery<R> createNamedReactiveSelectionQuery(String name);

	<R> ReactiveSelectionQuery<R> createNamedReactiveSelectionQuery(String name, Class<R> resultType);

	<R> ReactiveMutationQuery<R> createNamedReactiveMutationQuery(String name);

	@Deprecated(since = "6.0")
	@SuppressWarnings("rawtypes")
	<R> ReactiveQuery getNamedReactiveQuery(String queryName);

	@Deprecated(since = "6.0")
	@SuppressWarnings("rawtypes")
	<R> ReactiveNativeQuery getNamedReactiveNativeQuery(String name);

	@Deprecated(since = "6.0")
	@SuppressWarnings("rawtypes")
	ReactiveNativeQuery getNamedReactiveNativeQuery(String name, String resultSetMapping);
}
