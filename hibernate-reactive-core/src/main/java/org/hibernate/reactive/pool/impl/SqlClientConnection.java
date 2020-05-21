package org.hibernate.reactive.pool.impl;

import io.vertx.sqlclient.*;
import org.hibernate.reactive.adaptor.impl.ResultSetAdaptor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.util.impl.CompletionStages;

import java.sql.ResultSet;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;

/**
 * A reactive connection based on Vert.x's {@link SqlConnection}.
 */
public class SqlClientConnection implements ReactiveConnection {
	
	private static PropertyKind<Long> mySqlLastInsertedId;

	private final boolean showSQL;

	private final SqlConnection connection;
	private Transaction transaction;
	
	SqlClientConnection(SqlConnection connection, boolean showSQL) {
		this.showSQL = showSQL;
		this.connection = connection;
	}

	@Override
	public CompletionStage<Integer> update(String sql, Object[] paramValues) {
		return update( sql, Tuple.wrap( paramValues ) );
	}

	@Override
	public CompletionStage<Long> updateReturning(String sql, Object[] paramValues) {
		return updateReturning( sql, Tuple.wrap( paramValues ) );
	}

	@Override
	public CompletionStage<Long> selectLong(String sql, Object[] paramValues) {
		return preparedQuery( sql, Tuple.wrap( paramValues ) )
				.thenApply( rowSet -> {
					for (Row row: rowSet) {
						return row.getLong(0);
					}
					return null;
				} );
	}

	@Override
	public CompletionStage<Result> select(String sql) {
		return preparedQuery( sql ).thenApply(RowSetResult::new);
	}

	@Override
	public CompletionStage<Result> select(String sql, Object[] paramValues) {
		return preparedQuery( sql, Tuple.wrap( paramValues ) ).thenApply(RowSetResult::new);
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
		return preparedQuery( sql, Tuple.wrap( paramValues ) ).thenApply(ResultSetAdaptor::new);
	}

	@Override
	public CompletionStage<Void> execute(String sql) {
		return preparedQuery( sql ).thenApply( ignore -> null );
	}

	@Override
	public CompletionStage<Integer> update(String sql) {
		return preparedQuery( sql ).thenApply(SqlResult::rowCount);
	}

	public CompletionStage<Integer> update(String sql, Tuple parameters) {
		return preparedQuery( sql, parameters ).thenApply(SqlResult::rowCount);
	}

	public CompletionStage<Long> updateReturning(String sql, Tuple parameters) {
		return preparedQuery( sql, parameters )
				.thenApply( rows -> {
					RowIterator<Row> iterator = rows.iterator();
					return iterator.hasNext() ?
							iterator.next().getLong(0) :
							rows.property(getMySqlLastInsertedId());
				} );
	}

	public CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple parameters) {
		Objects.requireNonNull( sql, "SQL query cannot be null" );
		if (showSQL) {
			System.out.println(sql);
		}
		return Handlers.toCompletionStage(
				handler -> client().preparedQuery( sql ).execute( parameters, handler )
		);
	}

	public CompletionStage<RowSet<Row>> preparedQuery(String sql) {
		Objects.requireNonNull( sql, "SQL query cannot be null" );
		if (showSQL) {
			System.out.println(sql);
		}
		return Handlers.toCompletionStage(
				handler -> client().preparedQuery( sql ).execute( handler )
		);
	}


	private SqlClient client() {
		return transaction != null ? transaction : connection;
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		transaction = connection.begin();
		return CompletionStages.nullFuture();
//		return execute("begin");
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return Handlers.toCompletionStage(
				handler -> transaction.commit(
						ar -> {
							transaction = null;
							handler.handle( ar );
						}
				)
		);
//		return execute("commit");
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		return Handlers.toCompletionStage(
				handler -> transaction.rollback(
						ar -> {
							transaction = null;
							handler.handle( ar );
						}
				)
		);
//		return execute("rollback");
	}

	@Override
	public void close() {
		connection.close();
	}

	/**
	 * Loads MySQLClient.LAST_INSERTED_ID via reflection to avoid a hard
	 * dependency on the MySQL driver 
	 */
	@SuppressWarnings("unchecked")
	private static PropertyKind<Long> getMySqlLastInsertedId() {
	    if (mySqlLastInsertedId == null) {
            try {
                Class<?> MySQLClient = Class.forName( "io.vertx.mysqlclient.MySQLClient" );
                mySqlLastInsertedId = (PropertyKind<Long>) MySQLClient.getField( "LAST_INSERTED_ID" ).get( null );
            } catch (ClassNotFoundException | NoSuchFieldException | IllegalArgumentException | IllegalAccessException | SecurityException e) {
                throw new RuntimeException( "Unable to obtain MySQLClient.LAST_INSERTED_ID field", e );
            }
        }
        return mySqlLastInsertedId;
    }

	private static class RowSetResult implements Result {
		private final RowSet<Row> rowset;
		private final RowIterator<Row> it;

		public RowSetResult(RowSet<Row> rowset) {
			this.rowset = rowset;
			it = rowset.iterator();
		}

		@Override
		public int size() {
			return rowset.size();
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public Object[] next() {
			Row row = it.next();
			Object[] result = new Object[ row.size() ];
			for (int i=0; i<result.length; i++) {
				result[i] = row.getValue(i);
			}
			return result;
		}
	}
}
