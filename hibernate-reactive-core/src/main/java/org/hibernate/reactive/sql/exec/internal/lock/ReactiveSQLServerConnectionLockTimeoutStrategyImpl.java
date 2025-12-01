/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
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

/**
 * Reactive version of {@link org.hibernate.dialect.lock.internal.TransactSQLLockingSupport.SQLServerImpl}
 */
public class ReactiveSQLServerConnectionLockTimeoutStrategyImpl implements ReactiveConnectionLockTimeoutStrategy {

	public static final ReactiveSQLServerConnectionLockTimeoutStrategyImpl INSTANCE = new ReactiveSQLServerConnectionLockTimeoutStrategyImpl();

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
	public Level getSupportedLevel() {
		return ConnectionLockTimeoutStrategy.Level.EXTENDED;
	}

	@Override
	public CompletionStage<Timeout> getReactiveLockTimeout(
			ReactiveConnection connection,
			SessionFactoryImplementor factory) {
		return LockHelper.getLockTimeout(
				"select @@lock_timeout",
				ReactiveSQLServerConnectionLockTimeoutStrategyImpl::getTimeout,
				connection
		);
	}

	private static Timeout getTimeout(ReactiveConnection.Result resultSet) {
		final int millis = (int) resultSet.next()[0];
		return switch ( millis ) {
			case -1 -> WAIT_FOREVER;
			case 0 -> NO_WAIT;
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
					return milliseconds;
				},
				"set lock_timeout %s",
				connection
		);
	}

}
