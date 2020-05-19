package org.hibernate.reactive.service;

import java.util.concurrent.CompletionStage;

import io.vertx.sqlclient.Row;

import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.hibernate.reactive.impl.SqlClientConnection;

/**
 * Abstracts over reactive connection pools.
 *
 * @see SqlClientConnection
 */
// FIXME: We might need to replace RowSet and Tuple classes
public interface ReactiveConnection {

	CompletionStage<Void> execute(String sql);

	CompletionStage<Integer> update(String sql);

	CompletionStage<Integer> update(String sql, Tuple parameters);

	CompletionStage<RowSet<Row>> preparedQuery(String query);

	CompletionStage<Long> updateReturning(String sql, Tuple parameters);

	CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple parameters);

	CompletionStage<Void> beginTransaction();
	CompletionStage<Void> commitTransaction();
	CompletionStage<Void> rollbackTransaction();

	void close();

}

