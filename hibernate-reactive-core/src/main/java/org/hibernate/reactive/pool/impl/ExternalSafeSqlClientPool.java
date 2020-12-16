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
 * This is an alternative to {@link DefaultSqlClientPool},
 * meant to be used by integrating runtimes such as Quarkus,
 * and uses a supplier of source of Pool instances
 * as these need to be different in each Vert.x context.
 */
public final class ExternalSafeSqlClientPool extends SqlClientPool implements Stoppable {

	private final ThreadLocalPoolManager pools;
	private final SqlStatementLogger sqlStatementLogger;
	private final boolean usePostgresStyleParameters;

	public ExternalSafeSqlClientPool(Supplier<Pool> poolProducer, SqlStatementLogger sqlStatementLogger, boolean usePostgresStyleParameters) {
		Objects.requireNonNull( poolProducer );
		Objects.requireNonNull( sqlStatementLogger );
		this.pools = new ThreadLocalPoolManager( poolProducer );
		this.sqlStatementLogger = sqlStatementLogger;
		this.usePostgresStyleParameters = usePostgresStyleParameters;
	}

	@Override
	protected Pool getPool() {
		return pools.getOrStartPool();
	}

	@Override
	protected SqlStatementLogger getSqlStatementLogger() {
		return sqlStatementLogger;
	}

	@Override
	protected boolean usePostgresStyleParameters() {
		return usePostgresStyleParameters;
	}

	@Override
	public void stop() {
		pools.close();
	}

}
