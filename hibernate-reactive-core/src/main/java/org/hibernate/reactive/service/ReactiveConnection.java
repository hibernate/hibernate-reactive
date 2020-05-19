package org.hibernate.reactive.service;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import io.vertx.sqlclient.Row;

import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.hibernate.reactive.stage.Stage;

/**
 * Abstracts over reactive connection pools.
 *
 * @see org.hibernate.reactive.impl.PoolConnection
 */
// FIXME: We might need to replace RowSet and Tuple classes
public interface ReactiveConnection {
	CompletionStage<Void> inTransaction(
			Consumer<Stage.Session> consumer,
			Stage.Session delegate);

	CompletionStage<Integer> update(String sql);

	CompletionStage<Integer> update(String sql, Tuple parameters);

	CompletionStage<RowSet<Row>> preparedQuery(String query);

	CompletionStage<Long> updateReturning(String sql, Tuple parameters);

	CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple parameters);

	void close();

}

