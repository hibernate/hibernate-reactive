/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;

/**
 * A proxy {@link ReactiveConnection} that initializes the
 * underlying connection lazily.
 * <p>
 * Users of this class should primarily
 * {@link #withConnection(Function) <em>register demand</em>}
 * for a connection via and only trigger the process to
 * {@link #openConnection()} asynchronously acquire a connection}
 * when actually needed. The exact usage pattern however
 * depends on the reactive framework being used, because
 * {@link org.hibernate.reactive.mutiny.Mutiny} has, loosely
 * expressed, a strict separation of construction, separation
 * and execution whereas {@link CompletionStage} based approaches
 * do not have that separation and connections are rather
 * acquired eagerly (but still asynchronously).
 * <p>
 * The semantics of the {@link CompletionStage}s returned by
 * {@link #withConnection(Function)} and
 * {@link #openConnection()} are likely the same: all these
 * complete when either the connection is actually available
 * or an unrecoverable error has occurred.
 */
final class ProxyConnection implements ReactiveConnection {

	private final ReactiveConnectionPool sqlClientPool;
	private final String tenantId;

	/**
	 * All accesses to internal state are guarded with a
	 * {@code synchronized (sync)} to prevent race conditions. It is
	 * important to do this, because the callbacks from the connection
	 * pool back via {@link #connectionFromPool(ReactiveConnection)}
	 * do happen on different threads and can be concurrent to other
	 * operations. Proper fences and locking is required, so
	 * {@code synchronized} is fine.
	 */
	// Use own, private "sync" object.
	private final Object sync = new Object();

	/**
	 * The future used to actually acquire a connection from the pool.
	 * Do not expose this one externally, because that will be wrong.
	 * External code wants to rely on {@link #connectionCompletion}.
	 * This one is needed to a) know that the async acquire-connection
	 * operation has been started and b) to potentially cancel it, when
	 * this ProxyConnection instance is closed while the connection has
	 * not yet returned from the pool.
	 */
	// Accesses to 'connection' and 'state' are guarded by 'sync'.
	private ReactiveConnection connection;
	private int state = STATE_NOT_ACQUIRING;
	private static final int STATE_NOT_ACQUIRING = 0;
	private static final int STATE_ACQUIRING_CONNECTION = 1;
	private static final int STATE_ACQUIRED_CONNECTION = 2;
	private static final int STATE_CLOSED = 3;


	/**
	 * Accumulates all operations that need a {@link ReactiveConnection}. "Filled" via
	 * {@link #withConnection(Function)}.
	 */
	private final CompletableFuture<ReactiveConnection> connectionCompletion = new CompletableFuture<>();

	// This and the indirections via 'newInstance' should eventually get inlined by the JVM
	// (haven't checked though).
	private static BiFunction<ReactiveConnectionPool, String, ProxyConnection> newProxyConnection =
			ProxyConnection::new;
	// BEGIN OF test-related code
	// encapsulate the constructor call for test purposes
	static void setNewProxyConnectionWrapperForTests(BiFunction<ReactiveConnectionPool, String, ProxyConnection> newProxyConnection) {
		ProxyConnection.newProxyConnection = newProxyConnection;
	}
	static void resetNewProxyConnectionWrapperForTests() {
		ProxyConnection.newProxyConnection = ProxyConnection::new;
	}
	static ProxyConnection newInstanceForTests(ReactiveConnectionPool sqlClientPool, String tenantId) {
		return new ProxyConnection(sqlClientPool, tenantId);
	}
	// END OF test-related code

	static ProxyConnection newInstance(ReactiveConnectionPool sqlClientPool) {
		return newProxyConnection.apply(sqlClientPool, null);
	}

	static ProxyConnection newInstance(ReactiveConnectionPool sqlClientPool, String tenantId) {
		return newProxyConnection.apply(sqlClientPool, tenantId);
	}

	/**
	 * Do not use this constructor outside of this class and from test code.
	 */
	private ProxyConnection(ReactiveConnectionPool sqlClientPool, String tenantId) {
		this.sqlClientPool = sqlClientPool;
		this.tenantId = tenantId;
	}

	@Override
	public CompletionStage<ReactiveConnection> openConnection() {
		synchronized (sync) {
			switch (state) {
				case STATE_ACQUIRING_CONNECTION:
				case STATE_ACQUIRED_CONNECTION:
					// If the async operation to acquire the connection from the pool
					// has been started (--> openConnection != null), then return the
					// CompletableFuture/CompletionStage that will receive the connection
					// *after* the ProxyConnection's internal state has been properly
					// updated.
					return connectionCompletion;
				case STATE_CLOSED:
					// Need to handle this case separately, because connectionCompletion
					// would happily return the acquired connection, because that CF might
					// have already been completed (can't change the outcome of a CF once
					// it has been set).
					return ClosedReactiveConnection.completionStage();
			}

			state = STATE_ACQUIRING_CONNECTION;

			CompletionStage<ReactiveConnection> conn = tenantId == null ? sqlClientPool.getConnection() : sqlClientPool.getConnection(tenantId);

			// Seems that we can ignore the returned CompletionStage.
			// Hopefully that's not an implementation detail of something else...
			conn.thenApply( this::connectionFromPool );

			return connectionCompletion;
		}
	}

	private ReactiveConnection connectionFromPool(ReactiveConnection newConnection) {
		synchronized (sync) {
			// We do not need additional synchronization here, because the ordering of the
			// instructions make it safe to observe from other threads.
			if ( state != STATE_CLOSED ) {
				state = STATE_ACQUIRED_CONNECTION;
				this.connection = newConnection;
				this.connectionCompletion.complete( newConnection );
				return newConnection;
			}

			// This ProxyConnection has been closed before the connection was acquired.
			// There is not much we can do here, except tell the waiters that the owning
			// session has already been closed. Note that the exception won't be propagated,
			// if connectionCompletion's CompletableFuture is done.
			this.connectionCompletion.completeExceptionally( ClosedReactiveConnection.failure() );
			newConnection.close();
			// No need to return anything useful, because the result of the 'openConnection'
			// CompletableFuture is ignored.
			return null;
		}
	}

	<T> CompletionStage<T> withConnection(Function<ReactiveConnection, CompletionStage<T>> operation) {
		synchronized (sync) {
			if (state == STATE_CLOSED)
				throw ClosedReactiveConnection.failure();
			return connectionCompletion.thenCompose( operation );
		}
	}

	@Override
	public CompletionStage<Void> execute(String sql) {
		return withConnection( conn -> conn.execute( sql ) );
	}

	@Override
	public CompletionStage<Void> executeOutsideTransaction(String sql) {
		return withConnection( conn -> conn.executeOutsideTransaction( sql ) );
	}

	@Override
	public CompletionStage<Integer> update(String sql) {
		return withConnection( conn -> conn.update( sql ) );
	}

	@Override
	public CompletionStage<Integer> update(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.update( sql, paramValues ) );
	}

	@Override
	public CompletionStage<Void> update(
			String sql,
			Object[] paramValues,
			boolean allowBatching,
			Expectation expectation) {
		return withConnection( conn -> conn.update( sql, paramValues, false, expectation ) );
	}

	@Override
	public CompletionStage<int[]> update(String sql, List<Object[]> paramValues) {
		return withConnection( conn -> conn.update( sql, paramValues ) );
	}

	@Override
	public CompletionStage<Long> updateReturning(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.updateReturning( sql, paramValues ) );
	}

	@Override
	public CompletionStage<Result> select(String sql) {
		return withConnection( conn -> conn.select( sql ) );
	}

	@Override
	public CompletionStage<Result> select(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.select( sql, paramValues ) );
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.selectJdbc( sql, paramValues ) );
	}

	@Override
	public CompletionStage<Long> selectLong(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.selectLong( sql, paramValues ) );
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		return withConnection( ReactiveConnection::beginTransaction );
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return withConnection( ReactiveConnection::commitTransaction );
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		return withConnection( ReactiveConnection::rollbackTransaction );
	}

	@Override
	public CompletionStage<Void> executeBatch() {
		return withConnection( ReactiveConnection::executeBatch );
	}

	@Override
	public synchronized void close() {
		ReactiveConnection conn;
		synchronized (sync) {
			conn = connection;

			state = STATE_CLOSED;
			connection = null;
			// Tell all "consuming" completion-stages that this ProxyConnection's been closed.
			// (Nothing happens for completion-stages that are already completed.)
			connectionCompletion.completeExceptionally( ClosedReactiveConnection.failure() );

			// Note: do *NOT* cancel 'openConnection'.
			// Cancelling it will prevent that the "connection acquired" callback gets delivered
			// via the callback created in openConnection(), results in the acquired never being
			// closed and hanging around "forever". Aka: a connection-leak in overload-situations.
		}

		// Fine to actually close the connection outside of the synchronized block.
		if ( conn != null ) {
			// In case this throws any exception, that's maybe okay.
			// We probably do not need to care about the execution time of conn.close(), as
			// releasing a connection back to the pool should be rather quick.
			conn.close();
		}
	}
}
