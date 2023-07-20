/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool;

import org.hibernate.Incubating;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;
import org.hibernate.reactive.engine.impl.InternalStage;

import org.hibernate.Incubating;

import io.vertx.sqlclient.spi.DatabaseMetadata;

/**
 * Abstracts over reactive database connections, defining
 * operations that allow queries to be executed asynchronously
 * via {@link CompletionStage}.
 * <p>
 * It is illegal to perform two non-blocking operations concurrently
 * with a single {@code ReactiveConnection}. Instead, the second
 * operation must be chained on completion of the first operation.
 * This restriction might be relaxed in future, and is due to the
 * implementation of the {@code ProxyConnection}.
 *
 * @see ReactiveConnectionPool
 */
@Incubating
public interface ReactiveConnection {

	@FunctionalInterface
	interface Expectation {
		void verifyOutcome(int rowCount, int batchPosition, String sql);
	}

	DatabaseMetadata getDatabaseMetadata();

	InternalStage<Void> execute(String sql);

	InternalStage<Void> executeOutsideTransaction(String sql);

	/**
	 * Run sql as statement (instead of preparedStatement)
	 */
	InternalStage<Void> executeUnprepared(String sql);

	InternalStage<Integer> update(String sql);

	InternalStage<Integer> update(String sql, Object[] paramValues);

	InternalStage<Void> update(String sql, Object[] paramValues, boolean allowBatching, Expectation expectation);

	InternalStage<int[]> update(String sql, List<Object[]> paramValues);

	InternalStage<Result> select(String sql);

	InternalStage<Result> select(String sql, Object[] paramValues);

	InternalStage<ResultSet> selectJdbc(String sql, Object[] paramValues);

	/**
	 * This method is intended to be used only for queries returning
	 * a ResultSet that must be executed outside of any "current"
	 * transaction (i.e with autocommit=true).
	 * <p/>
	 * For example, it would be appropriate to use this method when
	 * performing queries on information_schema or system tables in
	 * order to obtain metadata information about catalogs, schemas,
	 * tables, etc.
	 *
	 * @param sql - the query to execute outside of a transaction
	 * @param paramValues - a non-null array of parameter values
	 *
	 * @return the InternalStage<ResultSet> from executing the query.
	 */
	InternalStage<ResultSet> selectJdbcOutsideTransaction(String sql, Object[] paramValues);

	<T> InternalStage<T> insertAndSelectIdentifier(String sql, Object[] paramValues, Class<T> idClass, String idColumnName);

	<T> InternalStage<T> selectIdentifier(String sql, Object[] paramValues, Class<T> idClass);

	interface Result extends Iterator<Object[]> {
		int size();
	}

	InternalStage<Void> beginTransaction();

	InternalStage<Void> commitTransaction();

	InternalStage<Void> rollbackTransaction();

	ReactiveConnection withBatchSize(int batchSize);

	InternalStage<Void> executeBatch();

	InternalStage<Void> close();
}
