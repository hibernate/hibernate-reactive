/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import org.hibernate.HibernateException;
import org.hibernate.dialect.lock.spi.ConnectionLockTimeoutStrategy;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;

import jakarta.persistence.Timeout;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.concurrent.CompletionStage;

import static org.hibernate.Timeouts.NO_WAIT;
import static org.hibernate.Timeouts.SKIP_LOCKED_MILLI;
import static org.hibernate.Timeouts.WAIT_FOREVER;
import static org.hibernate.Timeouts.WAIT_FOREVER_MILLI;

/**
 * Reactive version of {@link org.hibernate.dialect.lock.internal.MySQLLockingSupport.ConnectionLockTimeoutStrategyImpl}
 */
public class ReactiveMySQLConnectionLockTimeoutStrategyImpl implements ReactiveConnectionLockTimeoutStrategy {

	public static final ReactiveMySQLConnectionLockTimeoutStrategyImpl INSTANCE = new ReactiveMySQLConnectionLockTimeoutStrategyImpl();

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	public Timeout getLockTimeout(Connection connection, SessionFactoryImplementor factory) {
		throw LOG.nonReactiveMethodCall( "getReactiveLockTimeout()" );
	}

	@Override
	public void setLockTimeout(Timeout timeout, Connection connection, SessionFactoryImplementor factory) {
		throw LOG.nonReactiveMethodCall( "setReactiveLockTimeout()" );
	}

	@Override
	public ConnectionLockTimeoutStrategy.Level getSupportedLevel() {
		return ConnectionLockTimeoutStrategy.Level.EXTENDED;
	}

	@Override
	public CompletionStage<Timeout> getReactiveLockTimeout( ReactiveConnection connection, SessionFactoryImplementor factory) {
		return LockHelper.getLockTimeout(
				"SELECT @@SESSION.innodb_lock_wait_timeout",
				ReactiveMySQLConnectionLockTimeoutStrategyImpl::getTimeout,
				connection
		);
	}

	private static Timeout getTimeout(ReactiveConnection.Result resultSet) {
		// see https://dev.mysql.com/doc/refman/8.4/en/innodb-parameters.html#sysvar_innodb_lock_wait_timeout
		final int millis = (int) resultSet.next()[0];
		return switch ( millis ) {
			case 0 -> NO_WAIT;
			case 100000000 -> WAIT_FOREVER;
			default -> Timeout.milliseconds( millis );
		};
	}

	@Override
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
