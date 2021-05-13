/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.util.Objects;
import java.util.function.Supplier;

import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.service.spi.Stoppable;

import io.vertx.sqlclient.Pool;

/**
 * A pool of reactive connections backed by a supplier of
 * Vert.x {@link Pool} instances.
 * <p>
 * Each pool instance is kept into a separate ThreadLocal;
 * this implies minimum and maximum limits of each pool
 * are not honoured as global parameters, but within
 * each such instance.
 * <p>
 * This is an alternative to {@link DefaultSqlClientPool},
 * meant to be used by integrating runtimes such as Quarkus,
 * and uses a supplier of source of Pool instances
 * as these need to be different in each Vert.x context.
 */
public final class ExternalSafeSqlClientPool extends SqlClientPool implements Stoppable {

	private final Pool pools;
	private final SqlStatementLogger sqlStatementLogger;

	public ExternalSafeSqlClientPool(Supplier<Pool> poolProducer, SqlStatementLogger sqlStatementLogger) {
		Objects.requireNonNull( poolProducer );
		Objects.requireNonNull( sqlStatementLogger );
		this.pools = poolProducer.get();
		this.sqlStatementLogger = sqlStatementLogger;
	}

	@Deprecated
	public ExternalSafeSqlClientPool(Supplier<Pool> poolProducer, SqlStatementLogger sqlStatementLogger, boolean usePostgresStyleParameters) {
		this(poolProducer, sqlStatementLogger);
	}

	@Override
	protected Pool getPool() {
		return pools;
	}

	@Override
	protected SqlStatementLogger getSqlStatementLogger() {
		return sqlStatementLogger;
	}

	@Override
	public void stop() {
		pools.close();
	}

}
