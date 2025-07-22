/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool;


import org.hibernate.reactive.adaptor.impl.ResultSetAdaptor;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;


import io.vertx.sqlclient.spi.DatabaseMetadata;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A {@link ReactiveConnection} that automatically performs batching
 * of insert, update, and delete statements, relieving the client
 * persister code of the responsibility to manage batching. Actual
 * SQL statements are delegated to a given {@link ReactiveConnection}
 * which only supports explicit batching using {@link #update(String, List)}.
 * <p>
 * Note that in Hibernate core, the responsibilities of this class
 * are handled by {@link org.hibernate.engine.jdbc.spi.JdbcCoordinator}
 * and the {@link org.hibernate.engine.jdbc.batch.spi.Batch} interface.
 * However, the model used there is not easily adaptable to the reactive
 * paradigm.
 *
 * @author Gavin King
 */
public class BatchingConnection implements ReactiveConnection {

	private final ReactiveConnection delegate;
	private int batchSize;

	private String batchedSql;
	private Expectation batchedExpectation;
	private List<Object[]> batchParamValues;

	public BatchingConnection(ReactiveConnection delegate, int batchSize) {
		this.delegate = delegate;
		this.batchSize = batchSize;
	}

	@Override
	public DatabaseMetadata getDatabaseMetadata() {
		return delegate.getDatabaseMetadata();
	}

	@Override
	public boolean isTransactionInProgress() {
		return delegate.isTransactionInProgress();
	}

	@Override
	public ReactiveConnection withBatchSize(int batchSize) {
		if ( batchSize <= 1 ) {
			return delegate;
		}
		else {
			this.batchSize = batchSize;
			return this;
		}
	}

	@Override
	public CompletionStage<Void> executeBatch() {
		if ( !hasBatch() ) {
			return voidFuture();
		}
		else {
			String sql = batchedSql;
			Expectation expectation = batchedExpectation;
			List<Object[]> paramValues = batchParamValues;
			batchedSql = null;
			batchParamValues = null;
			batchedExpectation = null;

			if ( paramValues.size() == 1 ) {
				return update( sql, paramValues.get( 0 ) )
						.thenAccept( rowCount -> expectation.verifyOutcome( rowCount, -1, sql ) );
			}
			else {
				return update( sql, paramValues )
						.thenAccept( rowCounts -> {
							for ( int i = 0; i < rowCounts.length; i++ ) {
								expectation.verifyOutcome( rowCounts[i], i, sql );
							}
						} );
			}
		}
	}

	@Override
	public CompletionStage<Void> update(
			String sql, Object[] paramValues,
			boolean allowBatching, Expectation expectation) {
		if ( allowBatching && batchSize > 0 ) {
			if ( !hasBatch() ) {
				newBatch( sql, paramValues, expectation );
				return voidFuture();
			}
			else {
				if ( batchedSql.equals( sql ) && batchParamValues.size() < batchSize ) {
					batchParamValues.add( paramValues );
					return voidFuture();
				}
				else {
					return executeBatch()
							.thenAccept( v -> newBatch( sql, paramValues, expectation ) );
				}
			}
		}
		else {
			return delegate.update( sql, paramValues, false, expectation );
		}
	}

	private void newBatch(String sql, Object[] paramValues, Expectation expectation) {
		batchedSql = sql;
		batchedExpectation = expectation;
		batchParamValues = new ArrayList<>();
		batchParamValues.add( paramValues );
	}

	private boolean hasBatch() {
		return batchedSql != null;
	}

	@Override
	public CompletionStage<Void> execute(String sql) {
		return delegate.execute( sql );
	}

	@Override
	public CompletionStage<Void> executeUnprepared(String sql) {
		return delegate.executeUnprepared( sql );
	}

	@Override
	public CompletionStage<Void> executeOutsideTransaction(String sql) {
		return delegate.executeOutsideTransaction( sql );
	}

	@Override
	public CompletionStage<Integer> update(String sql) {
		return hasBatch()
				? executeBatch().thenCompose( v -> delegate.update( sql ) )
				: delegate.update( sql );
	}

	@Override
	public CompletionStage<Integer> update(String sql, Object[] paramValues) {
		return hasBatch()
				? executeBatch().thenCompose( v -> delegate.update( sql, paramValues ) )
				: delegate.update( sql, paramValues );
	}

	@Override
	public CompletionStage<int[]> update(String sql, List<Object[]> paramValues) {
		return hasBatch()
				? executeBatch().thenCompose( v -> delegate.update( sql, paramValues ) )
				: delegate.update( sql, paramValues );
	}

	public <T> CompletionStage<T> insertAndSelectIdentifier(String sql, Object[] paramValues, Class<T> idClass, String idColumnName) {
		return hasBatch()
				? executeBatch().thenCompose( v -> delegate.insertAndSelectIdentifier( sql, paramValues, idClass, idColumnName ) )
				: delegate.insertAndSelectIdentifier( sql, paramValues, idClass, idColumnName );
	}

	@Override
	public CompletionStage<ResultSet> insertAndSelectIdentifierAsResultSet(
			String sql,
			Object[] paramValues,
			Class<?> idClass,
			String idColumnName) {
		return insertAndSelectIdentifier( sql, paramValues, idClass, idColumnName )
				.thenApply( id -> new ResultSetAdaptor( id, idClass, idColumnName ) );
	}

	@Override
	public CompletionStage<ResultSet> executeAndSelectGeneratedValues(
			String sql,
			Object[] paramValues,
			List<Class<?>> idClasses,
			List<String> generatedColumnNames) {
		return hasBatch()
				? executeBatch().thenCompose( v -> delegate.executeAndSelectGeneratedValues( sql, paramValues, idClasses, generatedColumnNames ) )
				: delegate.executeAndSelectGeneratedValues( sql, paramValues, idClasses, generatedColumnNames );
	}

	@Override
	public CompletionStage<ReactiveConnection.Result> select(String sql) {
		return hasBatch()
				? executeBatch().thenCompose( v -> delegate.select( sql ) )
				: delegate.select( sql );
	}

	@Override
	public CompletionStage<ReactiveConnection.Result> select(String sql, Object[] paramValues) {
		return hasBatch()
				? executeBatch().thenCompose( v -> delegate.select( sql, paramValues ) )
				: delegate.select( sql, paramValues );
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
		return hasBatch()
				? executeBatch().thenCompose( v -> delegate.selectJdbc( sql, paramValues ) )
				: delegate.selectJdbc( sql, paramValues );
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql) {
		return hasBatch()
				? executeBatch().thenCompose( v -> delegate.selectJdbc( sql ) )
				: delegate.selectJdbc( sql );
	}

	@Override
	public <T> CompletionStage<T> selectIdentifier(String sql, Object[] paramValues, Class<T> idClass) {
		// Do not want to execute the batch here
		// because we want to be able to select
		// multiple ids before sending off a batch
		// of insert statements
		return delegate.selectIdentifier( sql, paramValues, idClass );
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		return delegate.beginTransaction();
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return delegate.commitTransaction();
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		return delegate.rollbackTransaction();
	}

	@Override
	public CompletionStage<Void> close() {
		return delegate.close();
	}
}
