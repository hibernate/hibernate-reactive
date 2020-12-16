/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;

import io.vertx.core.Context;
import io.vertx.sqlclient.Pool;

/**
 * A pool of reactive connections backed by a Vert.x {@link Pool}.
 * <p>
 * This is an alternative to
 * {@link org.hibernate.reactive.pool.impl.DefaultSqlClientPool},
 * for use when one doesn't want Hibernate Reactive to manage the
 * lifecycle of the underlying {@code Pool}.
 * <p>
 * This implementation is meant to be used in Quarkus or other runtimes.
 * <p>
 *     N.B. the injected pool instance is required to be threadsafe,
 *     while the default implementation in Vert.x version 3 is not.
 *     So use this only by extensions of the default implementation
 *     which can deliver a different actual underlying {@link Pool}
 *     instance for each thread.
 *     All clients of the pool instances retrieved from the injected
 *     {@link Pool} instance are expected to be running on a Vert.x
 *     event loop.
 *     Integration tests in Hibernate Reactive run exclusively on the
 *     Vert.x event loop, however in practice this will need to be
 *     guaranteed by the integrating runtime as well.
 *     Alternatively, a valid integration mode which doesn't require
 *     wrapping the {@code Pool} instance with a {@code ThreadLocal}
 *     could be devised by having all of the use of the Hibernate
 *     Reactive's SessionFactory instance ({@link Stage.SessionFactory}
 *     or a {@link Mutiny.SessionFactory}) constrained within a single
 *     thread, running within the Vert.x event loop.
 * </p>
 */
public final class ExternalSqlClientPool extends SqlClientPool {

	private final Pool pool;
	private final SqlStatementLogger sqlStatementLogger;
	private final boolean usePostgresStyleParameters;

	public ExternalSqlClientPool(Pool pool, SqlStatementLogger sqlStatementLogger, boolean usePostgresStyleParameters) {
		this.pool = pool;
		this.sqlStatementLogger = sqlStatementLogger;
		this.usePostgresStyleParameters = usePostgresStyleParameters;
	}

	@Override
	protected Pool getPool() {
		//First, check that the requester is running within the EventLoop:
		if ( !Context.isOnEventLoopThread() ) {
			//Not using the InternalStateAssertions here as this check is more critical; need to ensure all production code actually adheres to this constraint.
			//On top of correctness (accessing the pool from a non-eventloop thread exposes us to race conditions), we also don't want to the ThreadLocal
			//to store more Pool references than the configured Vert.x threads.
			throw new IllegalStateException(
					"A Reactive SQL Client Pool can only be used from within a Vert.x context. You are using Hibernate Reactive within a thread which is not a Vert.x event loop." );
		}
		return pool;
	}

	@Override
	protected SqlStatementLogger getSqlStatementLogger() {
		return sqlStatementLogger;
	}

	@Override
	protected boolean usePostgresStyleParameters() {
		return usePostgresStyleParameters;
	}

}
