package org.hibernate.reactive.service;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import io.vertx.sqlclient.Row;

import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.reactive.impl.SqlClientConnection;

/**
 * Abstracts over reactive connection pools.
 *
 * @see SqlClientConnection for the Vert.x-based implementation
 */
public interface ReactiveConnection {

	CompletionStage<Void> execute(String sql);

	CompletionStage<Integer> update(String sql);
	CompletionStage<Integer> update(String sql, Object[] paramValues);
	CompletionStage<Long> updateReturning(String sql, Object[] paramValues);

	CompletionStage<Result> select(String sql);
	CompletionStage<Result> select(String sql, Object[] paramValues);
	CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues);
	CompletionStage<Long> selectLong(String sql, Object[] paramValues);

	interface Result extends Iterator<Object[]> {
		int size();
	}

	CompletionStage<Void> beginTransaction();
	CompletionStage<Void> commitTransaction();
	CompletionStage<Void> rollbackTransaction();

	void close();

}

