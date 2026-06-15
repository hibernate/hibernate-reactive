/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.pool.ReactiveConnection;

import org.junit.jupiter.api.Test;

import io.vertx.sqlclient.spi.DatabaseMetadata;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ExternalTransaction}.
 */
public class ExternalTransactionTest {

	@Test
	public void testMarkForRollback() {
		ExternalTransaction tx = new ExternalTransaction( stubConnection( true ) );
		assertThat( tx.isMarkedForRollback() ).isFalse();
		tx.markForRollback();
		assertThat( tx.isMarkedForRollback() ).isTrue();
	}

	@Test
	public void testIsTransactionInProgressDelegatesToConnection() {
		AtomicBoolean inProgress = new AtomicBoolean( true );
		ExternalTransaction tx = new ExternalTransaction( stubConnection( inProgress ) );

		assertThat( tx.isTransactionInProgress() ).isTrue();

		inProgress.set( false );
		assertThat( tx.isTransactionInProgress() ).isFalse();
	}

	@Test
	public void testNewInstanceIsNotMarkedForRollback() {
		ExternalTransaction tx = new ExternalTransaction( stubConnection( true ) );
		assertThat( tx.isMarkedForRollback() ).isFalse();
	}

	@Test
	public void testCurrentTransactionCachesWithinSameTransaction() {
		AtomicBoolean inProgress = new AtomicBoolean( true );
		AtomicReference<Object> txId = new AtomicReference<>( new Object() );
		ReactiveConnection conn = stubConnection( inProgress, txId );
		CurrentTransaction<Mutiny.Transaction> ct = new CurrentTransaction<>( conn );

		// Repeated calls within the same transaction should return the same instance
		Mutiny.Transaction tx1 = ct.get();
		Mutiny.Transaction tx2 = ct.get();
		assertThat( tx1 ).isNotNull().isSameAs( tx2 );
	}

	@Test
	public void testCurrentTransactionResetsOnTransactionChange() {
		AtomicBoolean inProgress = new AtomicBoolean( true );
		AtomicReference<Object> txId = new AtomicReference<>( new Object() );
		ReactiveConnection conn = stubConnection( inProgress, txId );
		CurrentTransaction<Mutiny.Transaction> ct = new CurrentTransaction<>( conn );

		// First transaction: get a reference and mark it
		Mutiny.Transaction tx1 = ct.get();
		assertThat( tx1 ).isNotNull();
		tx1.markForRollback();
		assertThat( tx1.isMarkedForRollback() ).isTrue();

		// Simulate commit + new begin: change the transaction identity
		// without an intermediate call to get() while no tx is active
		txId.set( new Object() );

		// Second transaction: must be a fresh instance without stale state
		Mutiny.Transaction tx2 = ct.get();
		assertThat( tx2 )
				.as( "Back-to-back transaction must return a new instance" )
				.isNotNull()
				.isNotSameAs( tx1 );
		assertThat( tx2.isMarkedForRollback() )
				.as( "New transaction must not inherit markedForRollback" )
				.isFalse();
	}

	private static ReactiveConnection stubConnection(boolean transactionInProgress) {
		return stubConnection(
				new AtomicBoolean( transactionInProgress ),
				new AtomicReference<>( transactionInProgress ? new Object() : null )
		);
	}

	private static ReactiveConnection stubConnection(AtomicBoolean transactionInProgress) {
		return stubConnection(
				transactionInProgress,
				new AtomicReference<>( transactionInProgress.get() ? new Object() : null )
		);
	}

	private static ReactiveConnection stubConnection(AtomicBoolean transactionInProgress, AtomicReference<Object> txId) {
		return new ReactiveConnection() {
			@Override
			public boolean isTransactionInProgress() {
				return transactionInProgress.get();
			}

			@Override
			public Object currentTransactionId() {
				return txId.get();
			}

			@Override
			public DatabaseMetadata getDatabaseMetadata() {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Void> execute(String sql) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Void> executeOutsideTransaction(String sql) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Void> executeUnprepared(String sql) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Integer> update(String sql) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Integer> update(String sql, Object[] paramValues) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Void> update(String sql, Object[] paramValues, boolean allowBatching, Expectation expectation) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<int[]> update(String sql, List<Object[]> paramValues) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Result> select(String sql) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Result> select(String sql, Object[] paramValues) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
				throw new UnsupportedOperationException();
			}

			@Override
			@SuppressWarnings("deprecation")
			public <T> CompletionStage<T> insertAndSelectIdentifier(String sql, Object[] paramValues, Class<T> idClass, String idColumnName) {
				throw new UnsupportedOperationException();
			}

			@Override
			@SuppressWarnings("deprecation")
			public CompletionStage<ResultSet> insertAndSelectIdentifierAsResultSet(String sql, Object[] paramValues, Class<?> idClass, String idColumnName) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<ResultSet> selectJdbc(String sql) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<ResultSet> executeAndSelectGeneratedValues(String sql, Object[] paramValues, List<Class<?>> idClass, List<String> generatedColumnName) {
				throw new UnsupportedOperationException();
			}

			@Override
			public <T> CompletionStage<T> selectIdentifier(String sql, Object[] paramValues, Class<T> idClass) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Void> beginTransaction() {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Void> commitTransaction() {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Void> rollbackTransaction() {
				throw new UnsupportedOperationException();
			}

			@Override
			public ReactiveConnection withBatchSize(int batchSize) {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Void> executeBatch() {
				throw new UnsupportedOperationException();
			}

			@Override
			public CompletionStage<Void> close() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
