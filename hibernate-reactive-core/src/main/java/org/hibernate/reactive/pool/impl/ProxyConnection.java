/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;

/**
 * A proxy {@link ReactiveConnection} that initializes the
 * underlying connection lazily.
 */
final class ProxyConnection implements ReactiveConnection {

	private final ReactiveConnectionPool sqlClientPool;
	private final String tenantId;

	// 'connection' must be volatile, because it can be read from a different thread
	// than the one that writes this field - and read against this field are _not_
	// guarded with a 'synchronized(stateChange)'.
	private volatile ReactiveConnection connection;

	// Helper fields to track the state of this proxy-connection.
	private CompletionStage<ReactiveConnection> connectionCompletionStage;
	private boolean closed;
	private final Object stateChange = new Object();

	public ProxyConnection(ReactiveConnectionPool sqlClientPool) {
		this.sqlClientPool = sqlClientPool;
		tenantId = null;
	}

	public ProxyConnection(ReactiveConnectionPool sqlClientPool, String tenantId) {
		this.sqlClientPool = sqlClientPool;
		this.tenantId = tenantId;
	}

	<T> CompletionStage<T> withConnection(Function<ReactiveConnection, CompletionStage<T>> operation) {
		ReactiveConnection conn = connection;
		if (conn != null) {
			// Happy hot path: the connection's present and it can be used. When 'connection'
			// is not null, we're good to use it.
			//
			// A non-null 'connection' object can "leak" here, even if 'closed==true' in a
			// rare situation described below.
			// It feels fine to accept this exception, because 'close()' should only be
			// called when all operations have finished. And there's also a 2nd "line of
			// defense": the 'ReactiveConnection' itself is also closed and should error out
			// in that case.
			// There's also no guarantee that all consumers of 'connection' have finished when
			// 'close()' is being called.
			//
			// 		Thread A						Thread B
			//			withConnection():
			//			read 'connection' field
			//
			//			(thread suspended)				close() runs and finishes
			//
			//			runs operation.apply(conn)
			//
			return operation.apply(conn);
		}

		// No connection in the 'connection' field present.
		//
		// This can mean:
		// 1. this ProxyConnection has been closed (duh)
		// 2. no connection has been acquired from 'sqlClientPool'
		// 3. a connection is currently being acquired from 'sqlClientPool'

		// Tricky part comes next...
		// Either no connection has been acquired from the 'sqlClientPool' yet or a connection
		// is currently being acquired. Need to distinguish both cases and prevent that more than
		// one connection's being acquired from 'sqlClientPool'.
		//
		// Generally, the CompletionStage returned from 'sqlClientPool.getConnection()' is used
		// to "bundle" all outstanding 'operation's against the not-yet-available connection.
		//
		// Note: the 'connectionCompletionStage' is never cleared to 'null', because otherwise
		// a new race condition might be introduced. However, it is set to null, in 'close()',
		// because that's also guarded via 'stateChange'.
		//
		CompletionStage<ReactiveConnection> completionStage;
		synchronized (stateChange) {
			if (closed) {
				// easy case, just bark and bite
				throw new IllegalStateException("Proxy connection already closed");
			}

			completionStage = connectionCompletionStage;
			if (completionStage == null) {
				// First one getting here for this ProxyConnection instance.
				// Start acquiring a connection from 'sqlClientPool' and store the
				// CompletionStage instance for future use.
				CompletionStage<ReactiveConnection> connection =
						tenantId == null ? sqlClientPool.getConnection() : sqlClientPool.getConnection(tenantId);
				completionStage = connection.thenApply(newConnection -> {
					// Again, change to 'connection' guarded via 'stateChange'.
					synchronized (stateChange) {
						if (!closed) {
							this.connection = newConnection;
							return newConnection;
						}
						newConnection.close();
						// Return something that gives at least some descriptive exception
						return ClosedReactiveConnection.INSTANCE;
					}
				});
				connectionCompletionStage = completionStage;
			}
		}

		// "Queue up" the 'operation'.
		// If the connection's already available, the 'operation' will likely be executed
		// via this call site.
		// If the connection's not yet available, the 'operation' will be executed once
		// the 'completionStage' to acquire the connection has finished.
		return completionStage.thenCompose(operation);
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
	public void close() {
		ReactiveConnection conn = connection;
		try {
			synchronized (stateChange) {
				closed = true;
				connectionCompletionStage = null;
				connection = null;
			}
		}
		finally {
			if (conn != null) {
				conn.close();
			}
		}
	}
}
