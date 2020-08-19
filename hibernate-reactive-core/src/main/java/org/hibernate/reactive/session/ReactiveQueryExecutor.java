/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
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
    CompletionStage<Integer> executeReactiveUpdate(NativeSQLQuerySpecification specification,
                                                   QueryParameters parameters);

    <T> ResultSetMapping<T> getResultSetMapping(Class<T> resultType, String mappingName);

    void addBulkCleanupAction(BulkOperationCleanupAction action);
}
