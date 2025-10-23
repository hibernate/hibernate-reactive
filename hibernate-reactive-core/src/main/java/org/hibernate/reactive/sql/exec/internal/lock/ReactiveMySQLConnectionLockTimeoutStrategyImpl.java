/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import org.hibernate.HibernateException;
import org.hibernate.Timeouts;
import org.hibernate.dialect.lock.spi.ConnectionLockTimeoutStrategy;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.pool.ReactiveConnection;

import jakarta.persistence.Timeout;
import java.util.concurrent.CompletionStage;

import static org.hibernate.Timeouts.SKIP_LOCKED_MILLI;
import static org.hibernate.Timeouts.WAIT_FOREVER_MILLI;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * Reactive version of {@link org.hibernate.dialect.lock.internal.MySQLLockingSupport.ConnectionLockTimeoutStrategyImpl}
 */
public class ReactiveMySQLConnectionLockTimeoutStrategyImpl implements ReactiveConnectionLockTimeoutStrategy {

	public static final ReactiveMySQLConnectionLockTimeoutStrategyImpl INSTANCE = new ReactiveMySQLConnectionLockTimeoutStrategyImpl();

	@Override
	public ConnectionLockTimeoutStrategy.Level getSupportedLevel() {
		return ConnectionLockTimeoutStrategy.Level.EXTENDED;
	}

	@Override
	public CompletionStage<Timeout> getReactiveLockTimeout( ReactiveConnection connection, SessionFactoryImplementor factory) {
		return LockHelper.getLockTimeout(
				"SELECT @@SESSION.innodb_lock_wait_timeout",
				(resultSet) -> {
					// see https://dev.mysql.com/doc/refman/8.4/en/innodb-parameters.html#sysvar_innodb_lock_wait_timeout
					final int millis = (int) resultSet.next()[0];
					return switch ( millis ) {
						case 0 -> completedFuture( Timeouts.NO_WAIT );
						case 100000000 -> completedFuture( Timeouts.WAIT_FOREVER );
						default -> completedFuture( Timeout.milliseconds( millis ) );
					};
				},
				connection
		);
	}

	public CompletionStage<Void> setReactiveLockTimeout(
			Timeout timeout,
			ReactiveConnection connection,
			SessionFactoryImplementor factory) {
		return LockHelper.setLockTimeout(
				timeout,
				(t) -> {
					// see https://dev.mysql.com/doc/refman/8.4/en/innodb-parameters.html#sysvar_innodb_lock_wait_timeout
					final int milliseconds = timeout.milliseconds();
					if ( milliseconds == SKIP_LOCKED_MILLI ) {
						throw new HibernateException( "Connection lock-timeout does not accept skip-locked" );
					}
					if ( milliseconds == WAIT_FOREVER_MILLI ) {
						return 100000000;
					}
					return milliseconds;
				},
				"SET @@SESSION.innodb_lock_wait_timeout = %s",
				connection
		);
	}
}
