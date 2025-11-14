/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import org.hibernate.dialect.lock.internal.CockroachLockingSupport;
import org.hibernate.dialect.lock.internal.MySQLLockingSupport;
import org.hibernate.dialect.lock.internal.PostgreSQLLockingSupport;
import org.hibernate.dialect.lock.internal.TransactSQLLockingSupport;
import org.hibernate.dialect.lock.spi.ConnectionLockTimeoutStrategy;

/**
 * Builder to create {@link ReactiveConnectionLockTimeoutStrategy} equivalents of {@link ConnectionLockTimeoutStrategy}
 */
public class ReactiveConnectionLockTimeoutStrategyBuilder {
	public static ReactiveConnectionLockTimeoutStrategy build(ConnectionLockTimeoutStrategy strategy) {
		if ( strategy instanceof MySQLLockingSupport.ConnectionLockTimeoutStrategyImpl ) {
			return ReactiveMySQLConnectionLockTimeoutStrategyImpl.INSTANCE;
		}
		else if ( strategy instanceof CockroachLockingSupport ) {
			return ReactiveCockroachConnectionLockTimeoutStrategyImpl.INSTANCE;
		}
		else if ( strategy instanceof TransactSQLLockingSupport.SQLServerImpl ) {
			return ReactiveSQLServerConnectionLockTimeoutStrategyImpl.INSTANCE;
		}
		else if ( strategy instanceof PostgreSQLLockingSupport ) {
			return ReactivePostgreSQLConnectionLockTimeoutStrategyImpl.INSTANCE;
		}
		else {
			throw new IllegalArgumentException( "Unsupported ConnectionLockTimeoutStrategy: " + strategy.getClass()
					.getName() );
		}
	}
}
