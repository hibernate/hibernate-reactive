/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

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

	boolean isTransactionInProgress();

	@FunctionalInterface
	interface Expectation {
		void verifyOutcome(int rowCount, int batchPosition, String sql);
	}

	DatabaseMetadata getDatabaseMetadata();

	CompletionStage<Void> execute(String sql);

	CompletionStage<Void> executeOutsideTransaction(String sql);

	/**
	 * Run sql as statement (instead of preparedStatement)
	 */
	CompletionStage<Void> executeUnprepared(String sql);

	CompletionStage<Integer> update(String sql);

	CompletionStage<Integer> update(String sql, Object[] paramValues);

	CompletionStage<Void> update(String sql, Object[] paramValues, boolean allowBatching, Expectation expectation);

	CompletionStage<int[]> update(String sql, List<Object[]> paramValues);

	CompletionStage<Result> select(String sql);

	CompletionStage<Result> select(String sql, Object[] paramValues);

	CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues);

	/**
	 * @deprecated without substitution
	 */
	@Deprecated
	<T> CompletionStage<T> insertAndSelectIdentifier(String sql, Object[] paramValues, Class<T> idClass, String idColumnName);

	/**
	 * @deprecated use {@link #executeAndSelectGeneratedValues(String, Object[], List, List)}
	 */
	@Deprecated
	CompletionStage<ResultSet> insertAndSelectIdentifierAsResultSet(String sql, Object[] paramValues, Class<?> idClass, String idColumnName);


	CompletionStage<ResultSet> selectJdbc(String sql);

	CompletionStage<ResultSet> executeAndSelectGeneratedValues(String sql, Object[] paramValues, List<Class<?>> idClass, List<String> generatedColumnName);

	<T> CompletionStage<T> selectIdentifier(String sql, Object[] paramValues, Class<T> idClass);

	interface Result extends Iterator<Object[]> {
		int size();
	}

	CompletionStage<Void> beginTransaction();

	CompletionStage<Void> commitTransaction();

	CompletionStage<Void> rollbackTransaction();

	ReactiveConnection withBatchSize(int batchSize);

	CompletionStage<Void> executeBatch();

	CompletionStage<Void> close();
}
