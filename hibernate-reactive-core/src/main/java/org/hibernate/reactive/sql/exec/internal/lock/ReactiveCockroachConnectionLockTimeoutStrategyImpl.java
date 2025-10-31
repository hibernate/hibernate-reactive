/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import org.hibernate.HibernateException;
import org.hibernate.dialect.lock.spi.ConnectionLockTimeoutStrategy;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.pool.ReactiveConnection;

import jakarta.persistence.Timeout;
import java.util.concurrent.CompletionStage;

import static org.hibernate.Timeouts.NO_WAIT_MILLI;
import static org.hibernate.Timeouts.SKIP_LOCKED_MILLI;
import static org.hibernate.Timeouts.WAIT_FOREVER;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

public class ReactiveCockroachConnectionLockTimeoutStrategyImpl
		implements ReactiveConnectionLockTimeoutStrategy {

	public static final ReactiveCockroachConnectionLockTimeoutStrategyImpl INSTANCE = new ReactiveCockroachConnectionLockTimeoutStrategyImpl();

	@Override
	public Level getSupportedLevel() {
		return ConnectionLockTimeoutStrategy.Level.SUPPORTED;
	}

	@Override
	public CompletionStage<Timeout> getReactiveLockTimeout(
			ReactiveConnection connection,
			SessionFactoryImplementor factory) {
		return LockHelper.getLockTimeout(
				"show lock_timeout",
				(resultSet) -> {
					// see https://dev.mysql.com/doc/refman/8.4/en/innodb-parameters.html#sysvar_innodb_lock_wait_timeout
					final int millis = (int) resultSet.next()[0];
					return switch ( millis ) {
						case 0 -> completedFuture( WAIT_FOREVER );
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
					if ( milliseconds == NO_WAIT_MILLI ) {
						throw new HibernateException( "Connection lock-timeout does not accept no-wait" );
					}
					return milliseconds;
				},
				"set lock_timeout = %s",
				connection
		);
	}

}
