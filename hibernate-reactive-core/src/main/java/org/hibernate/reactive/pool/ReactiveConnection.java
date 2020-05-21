package org.hibernate.reactive.pool;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

/**
 * Abstracts over reactive database connections, defining
 * operations that allow queries to be executed asynchronously
 * via {@link CompletionStage}.
 *
 * @see ReactiveConnectionPool
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

