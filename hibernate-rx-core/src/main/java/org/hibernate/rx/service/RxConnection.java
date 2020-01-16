package org.hibernate.rx.service;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import io.vertx.axle.sqlclient.Row;
import org.hibernate.rx.RxSession;

import io.vertx.axle.sqlclient.RowSet;
import io.vertx.axle.sqlclient.Tuple;

// FIXME: We might need to replace RowSet and Tuple classes
public interface RxConnection {
	CompletionStage<Void> inTransaction(
			Consumer<RxSession> consumer,
			RxSession delegate);

	CompletionStage<Integer> update(String sql);

	CompletionStage<Integer> update(String sql, Tuple asTuple);

	CompletionStage<RowSet<Row>> preparedQuery(String query);

	CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple asTuple);

	void close();

}

